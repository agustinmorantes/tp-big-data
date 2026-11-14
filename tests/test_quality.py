from datetime import datetime

from pyspark.sql import functions as F

from cpa.quality import apply_billing_rules, apply_event_rules, split_duplicates

EVENT_COLS = ("event_id string, event_ts timestamp, org_id string, service string, region string, metric string, "
              "value string, unit string, cost_usd_increment double, _corrupt_record string, "
              "ingest_ts timestamp, source_file string")
TS = datetime(2025, 8, 10, 10, 0)


def event(event_id="e1", service="compute", region="us-east", metric="requests", value="10", unit="count",
          cost=1.0, corrupt=None, ts=TS, org="org_a", source="f1"):
    return (event_id, ts, org, service, region, metric, value, unit, cost, corrupt, TS, source)


def check(spark, rows):
    df = spark.createDataFrame(rows, EVENT_COLS)
    out = apply_event_rules(df, cost_min=-0.01)
    return {r["event_id"]: r for r in out.collect()}


def test_valid_event_passes_all_rules(spark):
    r = check(spark, [event()])["e1"]
    assert r["reject_reasons"] == [] and r["remediations"] == []
    assert r["value_num"] == 10.0 and r["is_negative_cost"] is False


def test_unit_null_with_value_is_remediated_from_metric_catalog(spark):
    r = check(spark, [event(metric="cpu_hours", unit=None, value="2.5")])["e1"]
    assert r["reject_reasons"] == []
    assert r["remediations"] == ["unit_imputed_from_metric"]
    assert r["unit"] == "hours" and r["unit_raw"] is None and r["unit_imputed"] is True


def test_negative_cost_is_flagged_not_rejected(spark):
    rows = check(spark, [event("neg", cost=-3.0), event("tiny", cost=-0.005)])
    assert rows["neg"]["is_negative_cost"] is True and rows["neg"]["reject_reasons"] == []
    assert rows["tiny"]["is_negative_cost"] is False  # dentro de la tolerancia de -0.01


def test_blocking_rules(spark):
    rows = check(spark, [
        event("bad_value", value="abc"),
        event("bad_service", service="quantum"),
        event("bad_region", region="mars-1"),
        event("bad_metric", metric="gpu_hours"),
        event("mismatch", metric="cpu_hours", unit="count"),
        event("no_ts", ts=None),
        event("corrupt", corrupt="{broken"),
    ])
    assert rows["bad_value"]["reject_reasons"] == ["value_not_numeric"]
    assert rows["bad_service"]["reject_reasons"] == ["unknown_service"]
    assert rows["bad_region"]["reject_reasons"] == ["unknown_region"]
    assert "unknown_metric" in rows["bad_metric"]["reject_reasons"]
    assert rows["mismatch"]["reject_reasons"] == ["unit_metric_mismatch"]
    assert rows["no_ts"]["reject_reasons"] == ["event_ts_invalid"]
    assert "corrupt_record" in rows["corrupt"]["reject_reasons"]


def test_null_event_id_rejected(spark):
    df = spark.createDataFrame([event(event_id=None)], EVENT_COLS)
    r = apply_event_rules(df, cost_min=-0.01).first()
    assert r["reject_reasons"] == ["event_id_null"]


def test_split_duplicates_in_batch_and_against_existing(spark):
    df = spark.createDataFrame([event("a", source="f1"), event("a", source="f2"), event("b"), event("c")], EVENT_COLS)
    existing = spark.createDataFrame([("c",)], "event_id string")
    new, dups = split_duplicates(df, existing)
    assert sorted(r["event_id"] for r in new.collect()) == ["a", "b"]
    assert new.filter("event_id = 'a'").first()["source_file"] == "f1"  # se queda el primero
    reasons = sorted((r["event_id"], r["_dup_reason"]) for r in dups.collect())
    assert reasons == [("a", "duplicate_event_id_in_batch"), ("c", "duplicate_event_id_already_in_silver")]


BILLING_COLS = ("invoice_id string, org_id string, month date, subtotal double, credits double, taxes double, "
                "currency string, exchange_rate_to_usd double, _corrupt_record string")


def test_billing_sign_fx_and_credits(spark):
    from datetime import date
    m = date(2025, 7, 1)
    df = spark.createDataFrame([
        ("i1", "o", m, -100.0, None, 21.0, "USD", 0.97, None),     # signo corregible + fx forzada + créditos nulos
        ("i2", "o", m, -100.0, 5.0, 30.0, "EUR", 1.1, None),       # negativo sin ratio 0.21 -> rechazo
        ("i3", "o", m, 1000.0, 10.0, 210.0, "ARS", 0.0015, None),  # ok
        ("i4", "o", m, 50.0, 0.0, 10.5, "BRL", 0.2, None),         # moneda desconocida
    ], BILLING_COLS)
    rows = {r["invoice_id"]: r for r in apply_billing_rules(df, 0.21, 0.001).collect()}
    i1 = rows["i1"]
    assert i1["reject_reasons"] == []
    assert set(i1["remediations"]) == {"subtotal_sign_corrected", "usd_fx_forced_to_1", "credits_null_as_zero"}
    assert i1["subtotal"] == 100.0 and i1["credits"] == 0.0 and i1["fx_rate_applied"] == 1.0
    assert rows["i2"]["reject_reasons"] == ["negative_subtotal_unexplained"]
    assert rows["i3"]["reject_reasons"] == [] and rows["i3"]["fx_rate_applied"] == 0.0015
    assert rows["i4"]["reject_reasons"] == ["unknown_currency"]
