"""Landing (CSV) -> Bronze (Parquet) para las 7 fuentes batch.

Bronze tipa con esquema explícito, agrega columnas técnicas (`ingest_ts`, `source_file`,
`load_id`) y deduplica por clave primaria. No descarta filas inválidas (ADR-004): las que no
parsean quedan con `_corrupt_record` y se resuelven en Silver.

Idempotencia: escritura `overwrite` con partitionOverwriteMode=dynamic. Re-ejecutar la carga
el mismo día pisa la misma partición (ingest_date / mes) en lugar de agregar filas.
"""
from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from cpa.common import RUN_ID, dir_stats, get_logger, record_metrics, write_parquet
from cpa.config import Config
from cpa.schemas import BATCH_SOURCES, CORRUPT, BatchSource, with_corrupt

log = get_logger("bronze_batch")


def read_landing_csv(spark: SparkSession, path: str, source: BatchSource) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT)
        .option("multiLine", False)
        .option("escape", '"')          # tags_json trae comillas escapadas como ""
        .schema(with_corrupt(source.ddl))
        .csv(path)
        # Antes del cache: sobre datos cacheados input_file_name() devuelve "".
        .withColumn("source_file", F.input_file_name())
    )


def add_technical_columns(df: DataFrame, ingest_ts: datetime, source: BatchSource) -> DataFrame:
    return (
        df.withColumn("ingest_ts", F.lit(ingest_ts).cast("timestamp"))
        .withColumn("load_id", F.lit(RUN_ID))
        .withColumn(source.partition_col, F.expr(source.partition_expr))
    )


def dedupe_by_pk(df: DataFrame, pk: tuple[str, ...]) -> DataFrame:
    """Una fila por clave primaria. Las filas con PK nula no se deduplican (irían todas a una
    sola) y se dejan pasar para que Silver las mande a quarantine."""
    has_pk = F.lit(True)
    for c in pk:
        has_pk = has_pk & F.col(c).isNotNull()
    w = Window.partitionBy(*pk).orderBy(F.col("ingest_ts").desc(), F.col("source_file"))
    with_pk = df.filter(has_pk).withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    return with_pk.unionByName(df.filter(~has_pk))


def ingest_source(spark: SparkSession, cfg: Config, source: BatchSource, ingest_ts: datetime) -> dict:
    src_path = str(cfg.landing / source.file)
    out_path = cfg.zone("bronze", source.name)
    raw = read_landing_csv(spark, src_path, source).cache()
    rows_in = raw.count()
    corrupt = raw.filter(F.col(CORRUPT).isNotNull()).count()

    bronze = dedupe_by_pk(add_technical_columns(raw, ingest_ts, source), source.primary_key)
    rows_out = bronze.count()
    write_parquet(bronze, out_path, partition_by=[source.partition_col])
    raw.unpersist()

    stats = {"source": source.name, "rows_in": rows_in, "rows_out": rows_out,
             "duplicates_removed": rows_in - rows_out, "corrupt_rows": corrupt, **dir_stats(out_path)}
    log.info("%-18s in=%-5d out=%-5d dup=%d corrupt=%d files=%d", source.name, rows_in, rows_out,
             rows_in - rows_out, corrupt, stats["parquet_files"])
    return stats


def run(spark: SparkSession, cfg: Config, sources: list[str] | None = None) -> list[dict]:
    ingest_ts = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    results = []
    for name in sources or list(BATCH_SOURCES):
        try:
            results.append(ingest_source(spark, cfg, BATCH_SOURCES[name], ingest_ts))
        except Exception:
            # Una fuente rota no tiene que tirar las demás: se registra y se sigue.
            log.exception("falló la ingesta de %s", name)
            results.append({"source": name, "error": True})
    record_metrics(cfg, "bronze_batch", sources=results)
    failed = [r["source"] for r in results if r.get("error")]
    if failed:
        raise RuntimeError(f"Fuentes con error en bronze_batch: {failed}")
    return results
