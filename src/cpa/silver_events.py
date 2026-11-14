"""Bronze -> Silver de eventos de uso, en streaming con foreachBatch.

Por micro-batch:
1. Reglas de calidad (quality.apply_event_rules) -> rechazos a quarantine.
2. Unicidad de event_id dentro del batch y contra lo ya publicado en Silver (idempotencia).
3. Política de datos tardíos (ADR-011): el evento se acepta y se marca `is_late` si su event_ts
   es anterior a (máximo event_ts visto en batches anteriores - umbral). Gold se recalcula por
   fecha, así que un tardío corrige el agregado en la siguiente corrida en lugar de perderse.
4. Enriquecimiento con customers_orgs y resources (Silver) y features por evento.
5. Append a Silver particionado por fecha de evento.

foreachBatch es at-least-once: si un batch se reintenta, el anti-join por event_id evita
duplicar Silver y la quarantine se escribe por (arranque de la query, `batch_id`) con overwrite dinámico.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from cpa.bronze_stream import BRONZE_TABLE, trigger_kwargs
from cpa.common import RUN_ID, dir_stats, get_logger, path_exists, record_metrics
from cpa.config import Config
from cpa.quality import REJECTED, REMEDIATED, apply_event_rules, split_duplicates, to_quarantine
from cpa.schemas import CORRUPT

log = get_logger("silver_events")
TABLE = "usage_events"

SILVER_COLUMNS = [
    "event_id", "event_ts", "event_date", "org_id", "org_name", "industry", "plan_tier", "is_enterprise",
    "resource_id", "resource_state", "service", "region", "metric", "value_num", "unit", "unit_imputed",
    "cost_usd", "is_negative_cost", "schema_version", "carbon_kg", "genai_tokens",
    "requests", "cpu_hours", "storage_gb_hours", "is_late", "batch_id", "ingest_ts", "source_file", "load_id",
]


class LateDataState:
    """Máximo event_ts procesado por batch, persistido junto al checkpoint.

    Guardarlo por batch_id hace que un batch reintentado use el mismo watermark que la primera
    vez (misma clasificación de tardíos) en vez del que dejó su propio intento fallido.
    """

    def __init__(self, path: Path):
        self.path = path

    def _load(self) -> dict[str, dict]:
        return json.loads(self.path.read_text()) if self.path.exists() else {}

    def max_before(self, batch_id: int) -> str | None:
        """Máximo event_ts de los batches anteriores a `batch_id` (None en el primero)."""
        prev = [v["max_event_ts"] for k, v in self._load().items() if int(k) < batch_id and v["max_event_ts"]]
        return max(prev) if prev else None

    def save(self, batch_id: int, max_event_ts: str | None) -> None:
        state = self._load()
        state[str(batch_id)] = {"max_event_ts": max_event_ts}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(state, indent=1))


def add_features(df: DataFrame) -> DataFrame:
    """Features por evento; Gold las suma por día. genai_tokens solo existe en v2 + servicio genai."""
    def metric_value(name: str):
        return F.when(F.col("metric") == name, F.col("value_num"))

    return (df.withColumn("cost_usd", F.col("cost_usd_increment"))
              .withColumn("requests", metric_value("requests"))
              .withColumn("cpu_hours", metric_value("cpu_hours"))
              .withColumn("storage_gb_hours", metric_value("storage_gb_hours"))
              .withColumn("genai_tokens", F.col("genai_tokens").cast("long"))
              .withColumn("event_date", F.to_date("event_ts")))


def enrich(df: DataFrame, orgs: DataFrame, resources: DataFrame) -> DataFrame:
    orgs = orgs.select("org_id", "org_name", "industry", "plan_tier", "is_enterprise")
    res = resources.select("resource_id", F.col("state").alias("resource_state"))
    return df.join(F.broadcast(orgs), "org_id", "left").join(F.broadcast(res), "resource_id", "left")


def process_batch(spark: SparkSession, cfg: Config, batch: DataFrame, batch_id: int,
                  stream_run: str = RUN_ID) -> dict:
    q, s = cfg.section("quality"), cfg.section("streaming")
    silver_path = cfg.zone("silver", TABLE)
    orgs = spark.read.parquet(cfg.zone("silver", "customers_orgs"))
    resources = spark.read.parquet(cfg.zone("silver", "resources"))

    known = orgs.select("org_id").withColumn("_org_known", F.lit(True))
    checked = apply_event_rules(batch.join(F.broadcast(known), "org_id", "left"), q["cost_min_usd"], "_org_known")
    checked = checked.cache()

    rejected = checked.filter(F.size("reject_reasons") > 0)
    valid = checked.filter(F.size("reject_reasons") == 0)
    # Foto de los event_id ya publicados. localCheckpoint corta el linaje con la ruta de Silver:
    # si no, al escribir este batch Spark invalida la caché y re-evalúa el anti-join contra el
    # Silver ya actualizado (todo el batch pasaría a verse como duplicado).
    existing = (spark.read.parquet(silver_path).select("event_id").localCheckpoint(eager=True)
                if path_exists(silver_path) else None)
    new, dups = split_duplicates(valid, existing)
    dups = dups.withColumn("reject_reasons", F.array("_dup_reason")).drop("_dup_reason")
    remediated = new.filter(F.size("remediations") > 0)

    # --- datos tardíos ---
    state = LateDataState(Path(cfg.checkpoint("silver_usage_events")) / "late_data_state.json")
    threshold = float(s.get("late_threshold_hours", 6))
    max_prev = state.max_before(batch_id)
    if max_prev is None:
        new = new.withColumn("is_late", F.lit(False))
    else:
        watermark = F.to_timestamp(F.lit(max_prev)) - F.expr(f"INTERVAL {int(threshold * 60)} MINUTES")
        new = new.withColumn("is_late", F.col("event_ts") < watermark)

    silver = (enrich(add_features(new), orgs, resources)
              .withColumn("batch_id", F.lit(batch_id))
              .select(*SILVER_COLUMNS)).cache()
    quarantine = (to_quarantine(rejected.drop("_org_known"), TABLE, REJECTED)
                  .unionByName(to_quarantine(dups.drop("_org_known"), TABLE, REJECTED))
                  .unionByName(to_quarantine(remediated.drop("_org_known"), TABLE, REMEDIATED, "remediations"))
                  .withColumn("batch_id", F.lit(batch_id))
                  # arranque de la query + batch_id: un reintento del mismo batch pisa su partición,
                  # y un arranque nuevo (p. ej. tras perder el checkpoint) no borra la quarantine anterior.
                  .withColumn("batch_key", F.lit(f"{stream_run}_{batch_id:05d}"))).cache()

    agg = silver.agg(F.count("*").alias("n"), F.max("event_ts").alias("max_ts"),
                     F.sum(F.col("is_late").cast("int")).alias("late"),
                     F.sum(F.col("is_negative_cost").cast("int")).alias("neg")).first()
    q_counts = {(r["disposition"], r["primary_reason"]): r["count"]
                for r in quarantine.groupBy("disposition", "primary_reason").count().collect()}

    (quarantine.coalesce(1).write.mode("overwrite").partitionBy("batch_key")
     .parquet(cfg.zone("quarantine", TABLE)))
    silver.repartition("event_date").write.mode("append").partitionBy("event_date").parquet(silver_path)

    max_ts = agg["max_ts"].isoformat(sep=" ") if agg["max_ts"] else None
    state.save(batch_id, max(filter(None, [max_ts, max_prev]), default=None))

    def total(disposition: str, prefix: str = "") -> int:
        return sum(n for (d, r), n in q_counts.items() if d == disposition and r.startswith(prefix))

    stats = {"batch_id": batch_id, "input": checked.count(), "silver_new": agg["n"],
             "rejected": total(REJECTED) - total(REJECTED, "duplicate_"),
             "duplicates": total(REJECTED, "duplicate_"), "remediated": total(REMEDIATED),
             "late_events": agg["late"] or 0, "negative_cost_flagged": agg["neg"] or 0,
             "max_event_ts_before_batch": max_prev,
             "quarantine_by_reason": {f"{d}:{r}": n for (d, r), n in q_counts.items()}}
    quarantine.unpersist()
    checked.unpersist()
    silver.unpersist()
    record_metrics(cfg, "silver_events_batch", **stats)
    return stats


def run(spark: SparkSession, cfg: Config) -> dict:
    s = cfg.section("streaming")
    bronze_path = cfg.zone("bronze", BRONZE_TABLE)
    bronze_schema = spark.read.parquet(bronze_path).schema
    stream = (spark.readStream.schema(bronze_schema)
              .option("maxFilesPerTrigger", s.get("max_files_per_trigger", 10))
              .parquet(bronze_path))

    stream_run = f"{RUN_ID}_{datetime.now(timezone.utc):%H%M%S%f}"
    query = (stream.writeStream.queryName("silver_usage_events")
             .foreachBatch(lambda df, bid: process_batch(spark, cfg, df, bid, stream_run))
             .option("checkpointLocation", cfg.checkpoint("silver_usage_events"))
             .trigger(**trigger_kwargs(s.get("trigger", "availableNow")))
             .start())
    query.awaitTermination()
    if query.exception():
        raise RuntimeError(f"silver_usage_events terminó con error: {query.exception()}")

    silver_path = cfg.zone("silver", TABLE)
    total = spark.read.parquet(silver_path).count() if path_exists(silver_path) else 0
    stats = {"silver_total_rows": total, **dir_stats(silver_path)}
    record_metrics(cfg, "silver_events", **stats)
    return stats


def compact(spark: SparkSession, cfg: Config) -> dict:
    """Compacta Silver de eventos a un archivo por fecha (R-5). Correr con el stream detenido.

    El replay genera un archivo por (micro-batch, fecha). Se reescribe a una ruta temporal y se
    intercambia: Spark no permite sobrescribir una ruta que está leyendo.
    """
    import shutil

    path = Path(cfg.zone("silver", TABLE))
    before = dir_stats(path)
    tmp = path.with_name(TABLE + "__compacting")
    shutil.rmtree(tmp, ignore_errors=True)
    df = spark.read.parquet(str(path))
    df.repartition("event_date").write.mode("overwrite").partitionBy("event_date").parquet(str(tmp))
    if spark.read.parquet(str(tmp)).count() != df.count():
        raise RuntimeError("La compactación cambió el conteo: se aborta sin tocar Silver")
    shutil.rmtree(path)
    tmp.rename(path)
    after = dir_stats(path)
    stats = {"files_before": before["parquet_files"], "files_after": after["parquet_files"],
             "bytes_before": before["bytes"], "bytes_after": after["bytes"]}
    record_metrics(cfg, "compact_silver_events", **stats)
    return stats
