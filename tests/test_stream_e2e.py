"""Bronze stream + Silver eventos de punta a punta sobre fixtures chicos.

Archivo 0: casos de calidad (válidos, remediado, negativo, rechazos, duplicado intra-batch, corrupto).
Archivo 1: un válido nuevo, un duplicado de un evento ya publicado y un evento tardío.
"""
import copy
import shutil
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from cpa import bronze_stream, silver_events
from cpa.config import Config, load_config

FIXTURES = Path(__file__).parent / "fixtures" / "landing"


@pytest.fixture()
def cfg(tmp_path, spark):
    raw = copy.deepcopy(load_config().raw)
    landing = tmp_path / "landing"
    shutil.copytree(FIXTURES, landing)
    raw["paths"].update(landing=str(landing), lake=str(tmp_path / "lake"), evidence=str(tmp_path / "evidence"))
    raw["streaming"]["max_files_per_trigger"] = 1  # un archivo por micro-batch
    c = Config(raw=raw, root=tmp_path)
    # Silver de maestros mínimo (silver_masters necesita las 7 fuentes; acá solo hacen falta dos).
    for name in ("customers_orgs", "resources"):
        spark.read.option("header", True).csv(str(landing / f"{name}.csv")).write.parquet(c.zone("silver", name))
    return c


def silver_ids(spark, cfg):
    return sorted(r["event_id"] for r in spark.read.parquet(cfg.zone("silver", "usage_events")).collect())


def test_stream_quality_dedupe_late_and_idempotency(spark, cfg):
    bronze = bronze_stream.run(spark, cfg)
    assert bronze["rows_ingested"] == 13
    silver_events.run(spark, cfg)

    expected = ["evt_dup_in_file", "evt_late", "evt_negative", "evt_ok_1", "evt_ok_2", "evt_unit_null"]
    assert silver_ids(spark, cfg) == expected

    events = {r["event_id"]: r for r in spark.read.parquet(cfg.zone("silver", "usage_events")).collect()}
    assert events["evt_late"]["is_late"] is True
    assert not any(events[e]["is_late"] for e in expected if e != "evt_late")
    assert events["evt_negative"]["is_negative_cost"] is True and events["evt_negative"]["genai_tokens"] == 900
    assert events["evt_unit_null"]["unit"] == "count" and events["evt_unit_null"]["unit_imputed"] is True
    assert events["evt_ok_1"]["org_name"] == "Org A" and events["evt_ok_1"]["resource_state"] == "running"

    q = spark.read.parquet(cfg.zone("quarantine", "usage_events"))
    reasons = {(r["disposition"], r["primary_reason"]): r["count"]
               for r in q.groupBy("disposition", "primary_reason").count().collect()}
    assert reasons[("rejected", "event_id_null")] == 1
    assert reasons[("rejected", "value_not_numeric")] == 1
    assert reasons[("rejected", "unknown_service")] == 1
    assert reasons[("rejected", "orphan_org")] == 1
    assert reasons[("rejected", "duplicate_event_id_in_batch")] == 1
    assert reasons[("rejected", "duplicate_event_id_already_in_silver")] == 1
    assert reasons[("remediated", "unit_imputed_from_metric")] == 1
    assert q.filter(F.array_contains("reasons", "corrupt_record")).count() == 1

    # Re-ejecución con checkpoints: no hay input nuevo, nada cambia.
    bronze_stream.run(spark, cfg)
    silver_events.run(spark, cfg)
    assert silver_ids(spark, cfg) == expected

    # Pérdida del checkpoint de Silver: se relee todo Bronze y el dedupe contra Silver evita duplicar.
    n_quarantine = spark.read.parquet(cfg.zone("quarantine", "usage_events")).count()
    shutil.rmtree(cfg.checkpoint("silver_usage_events"))
    silver_events.run(spark, cfg)
    assert silver_ids(spark, cfg) == expected
    # La quarantine anterior se conserva y se suman las 13 filas releídas (rechazos + duplicados).
    assert spark.read.parquet(cfg.zone("quarantine", "usage_events")).count() == n_quarantine + 13
