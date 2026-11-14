"""Landing (JSONL) -> Bronze con Structured Streaming, y vista de tiempo casi real con watermark.

Query 1 `bronze_usage_events` (sin estado): esquema explícito, PERMISSIVE + `_corrupt_record`,
columnas técnicas y sink Parquet con checkpoint. El sink de archivos + checkpoint es
exactly-once: un archivo de Landing ya procesado no se vuelve a leer, y un micro-batch que falla
a mitad se reescribe sin duplicar (`_spark_metadata`).

Bronze se particiona por fecha de **llegada** (`ingest_date`), no de evento: un evento tardío no
reabre particiones viejas y cada micro-batch escribe en una sola partición (ADR-010).

Query 2 `speed_cost_by_org_hour` (con estado): costo por organización en ventanas de 1 h con
`withWatermark`. Es la ruta de baja latencia del patrón híbrido; los eventos que llegan detrás
del watermark se cuentan en `numRowsDroppedByWatermark` y no modifican ventanas ya emitidas.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from cpa.common import RUN_ID, dir_stats, get_logger, record_metrics, remove_path
from cpa.config import Config
from cpa.schemas import USAGE_EVENTS_DDL

log = get_logger("bronze_stream")
BRONZE_TABLE = "usage_events"


def trigger_kwargs(trigger: str) -> dict:
    """'availableNow' -> procesa lo disponible y termina; 'processingTime=3 minutes' -> continuo."""
    if trigger == "availableNow":
        return {"availableNow": True}
    key, _, value = trigger.partition("=")
    return {key.strip(): value.strip()}


def read_landing_stream(spark: SparkSession, cfg: Config) -> DataFrame:
    s = cfg.section("streaming")
    return (
        spark.readStream.format("json")
        .schema(USAGE_EVENTS_DDL)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("maxFilesPerTrigger", s.get("max_files_per_trigger", 10))
        .load(str(cfg.landing / "usage_events_stream"))
    )


def to_bronze(df: DataFrame) -> DataFrame:
    """Columnas técnicas + event_ts parseado. No filtra nada (ADR-004)."""
    return (
        df.withColumn("event_ts", F.to_timestamp("timestamp"))
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("ingest_date", F.to_date("ingest_ts"))
        .withColumn("source_file", F.input_file_name())
        .withColumn("load_id", F.lit(RUN_ID))
    )


def run(spark: SparkSession, cfg: Config) -> dict:
    s = cfg.section("streaming")
    out_path = cfg.zone("bronze", BRONZE_TABLE)
    query = (
        to_bronze(read_landing_stream(spark, cfg))
        .writeStream.queryName("bronze_usage_events")
        .format("parquet")
        .outputMode("append")
        .option("path", out_path)
        .option("checkpointLocation", cfg.checkpoint("bronze_usage_events"))
        .partitionBy("ingest_date")
        .trigger(**trigger_kwargs(s.get("trigger", "availableNow")))
        .start()
    )
    log.info("query %s iniciada (trigger=%s)", query.name, s.get("trigger"))
    query.awaitTermination()
    progress = [p for p in query.recentProgress if p.get("numInputRows", 0) > 0]
    rows = sum(p["numInputRows"] for p in progress)
    stats = {"micro_batches_with_data": len(progress), "rows_ingested": rows, **dir_stats(out_path)}
    record_metrics(cfg, "bronze_stream", **stats)
    if query.exception():
        raise RuntimeError(f"bronze_usage_events terminó con error: {query.exception()}")
    return stats


def run_speed_view(spark: SparkSession, cfg: Config) -> dict:
    """Agregación con watermark sobre Bronze, a la tabla en memoria `speed_cost_by_org_hour`.

    Se ejecuta con availableNow y se reportan las filas descartadas por el watermark: con el
    replay del dataset (cada archivo cubre los 60 días) la mayoría de los eventos llegan
    "tarde" respecto de la ventana, que es justo lo que documenta el ADR-011.
    """
    s = cfg.section("streaming")
    bronze_schema = spark.read.parquet(cfg.zone("bronze", BRONZE_TABLE)).schema
    agg = (
        spark.readStream.schema(bronze_schema)
        .option("maxFilesPerTrigger", 1)
        .parquet(cfg.zone("bronze", BRONZE_TABLE))
        .filter(F.col("event_ts").isNotNull() & F.col("org_id").isNotNull())
        .withWatermark("event_ts", s.get("speed_watermark", "10 minutes"))
        .groupBy(F.window("event_ts", s.get("speed_window", "1 hour")), "org_id")
        .agg(F.round(F.sum("cost_usd_increment"), 4).alias("cost_usd"), F.count("*").alias("events"))
    )
    chk = cfg.checkpoint("speed_cost_by_org_hour")
    remove_path(chk)  # el sink en memoria no persiste: cada corrida arranca de cero
    query = (
        agg.writeStream.queryName("speed_cost_by_org_hour")
        .format("memory")
        .outputMode("update")
        .option("checkpointLocation", chk)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    progress = query.recentProgress
    dropped = sum(op.get("numRowsDroppedByWatermark", 0) for p in progress for op in p.get("stateOperators", []))
    input_rows = sum(p.get("numInputRows", 0) for p in progress)
    stats = {"input_rows": input_rows, "rows_dropped_by_watermark": dropped,
             "dropped_pct": round(100 * dropped / input_rows, 2) if input_rows else 0.0,
             "watermark_delay": s.get("speed_watermark"),
             "windows_in_memory": spark.table("speed_cost_by_org_hour").count()}
    record_metrics(cfg, "speed_view", **stats)
    return stats
