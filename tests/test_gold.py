from datetime import date, timedelta

from pyspark.sql import functions as F

from cpa.gold import cost_anomaly_mart, genai_tokens_by_org_date, org_daily_usage_by_service, top_services_rolling

SILVER_COLS = ("event_id string, org_id string, org_name string, event_date date, service string, cost_usd double, "
               "is_negative_cost boolean, is_late boolean, requests double, cpu_hours double, "
               "storage_gb_hours double, genai_tokens long, carbon_kg double")
D = date(2025, 8, 1)


def ev(i, org="o1", day=D, service="compute", cost=1.0, requests=None, tokens=None, carbon=None):
    return (f"e{i}", org, "Org", day, service, cost, cost < -0.01, False, requests, None, None, tokens, carbon)


def test_daily_mart_grain_and_sums(spark):
    rows = [ev(1, cost=2.0, requests=10.0, carbon=0.1), ev(2, cost=-1.0, requests=5.0),
            ev(3, service="storage", cost=4.0), ev(4, day=D + timedelta(days=1), cost=1.0)]
    daily = org_daily_usage_by_service(spark.createDataFrame(rows, SILVER_COLS))
    assert daily.count() == daily.select("org_id", "usage_date", "service").distinct().count() == 3
    r = daily.filter((F.col("usage_date") == D) & (F.col("service") == "compute")).first()
    assert r["daily_cost_usd"] == 1.0 and r["gross_cost_usd"] == 2.0
    assert r["requests"] == 15.0 and r["events"] == 2 and r["negative_cost_events"] == 1
    assert r["carbon_coverage"] == 0.5


def test_mad_flags_spike_and_not_normal_days(spark):
    costs = [10.0, 11.0, 9.5, 10.5, 10.2, 9.8, 10.1, 9.9, 10.3, 10.0, 9.7, 10.4, 10.6, 9.6, 300.0]
    rows = [("o1", D + timedelta(days=i), "compute", "Org", c, 1, 0) for i, c in enumerate(costs)]
    daily = spark.createDataFrame(rows, "org_id string, usage_date date, service string, org_name string, "
                                        "daily_cost_usd double, events long, negative_cost_events long")
    scored = cost_anomaly_mart(daily, threshold=3.5, min_days=14)
    flagged = [r["usage_date"] for r in scored.filter("is_anomaly").collect()]
    assert flagged == [D + timedelta(days=14)]
    assert scored.filter("is_anomaly").first()["anomaly_type"] == "spike"


def test_mad_requires_min_history(spark):
    rows = [("o1", D + timedelta(days=i), "compute", "Org", c, 1, 0) for i, c in enumerate([1.0, 1.0, 50.0])]
    daily = spark.createDataFrame(rows, "org_id string, usage_date date, service string, org_name string, "
                                        "daily_cost_usd double, events long, negative_cost_events long")
    assert cost_anomaly_mart(daily, 3.5, 14).filter("is_anomaly").count() == 0


def test_top_services_window_and_rank(spark):
    rows = [("o1", D, "compute", 5.0, 1.0), ("o1", D, "storage", 1.0, 1.0),
            ("o1", D + timedelta(days=1), "storage", 10.0, 1.0),
            ("o1", D - timedelta(days=20), "genai", 100.0, 1.0)]  # fuera de la ventana de 14 días
    daily = spark.createDataFrame(rows, "org_id string, usage_date date, service string, daily_cost_usd double, requests double")
    top = top_services_rolling(daily, 14).filter(F.col("as_of_date") == D + timedelta(days=1))
    ranked = [(r["cost_rank"], r["service"], r["cost_usd_window"]) for r in top.orderBy("cost_rank").collect()]
    assert ranked == [(1, "storage", 11.0), (2, "compute", 5.0)]


def test_genai_mart_coverage(spark):
    rows = [ev(1, service="genai", cost=2.0, tokens=1000), ev(2, service="genai", cost=1.0, tokens=None),
            ev(3, service="compute", cost=9.0)]
    g = genai_tokens_by_org_date(spark.createDataFrame(rows, SILVER_COLS)).first()
    assert g["genai_tokens"] == 1000 and g["genai_cost_usd"] == 3.0 and g["token_coverage"] == 0.5
    assert g["cost_per_1k_tokens_usd"] == 3.0
