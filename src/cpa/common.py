"""Sesión de Spark, logging, métricas de corrida y utilidades de E/S."""
from __future__ import annotations

import json
import logging
import shutil
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from cpa.config import Config

LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(f"cpa.{name}")


def setup_logging(cfg: Config) -> Path:
    """Log a consola y a archivo (logs/ está ignorado por git; la evidencia se copia aparte)."""
    log_dir = cfg.root / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"pipeline_{RUN_ID}.log"
    root = logging.getLogger("cpa")
    if not root.handlers:
        root.setLevel(logging.INFO)
        for handler in (logging.StreamHandler(sys.stdout), logging.FileHandler(log_file, encoding="utf-8")):
            handler.setFormatter(logging.Formatter(LOG_FORMAT))
            root.addHandler(handler)
    return log_file


def get_spark(cfg: Config, app_name: str = "cloud-provider-analytics") -> SparkSession:
    s = cfg.section("spark")
    spark = (
        SparkSession.builder.appName(app_name)
        .master(s.get("master", "local[*]"))
        .config("spark.driver.memory", s.get("driver_memory", "2g"))
        .config("spark.sql.shuffle.partitions", s.get("shuffle_partitions", 8))
        .config("spark.sql.session.timeZone", s.get("timezone", "UTC"))
        # Reescribir solo las particiones presentes en el DataFrame: re-ejecutar una carga
        # pisa sus particiones en vez de duplicarlas (idempotencia de las capas batch).
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def path_exists(path: str | Path) -> bool:
    p = Path(path)
    return p.exists() and any(p.iterdir()) if p.is_dir() else p.exists()


def remove_path(path: str | Path) -> None:
    shutil.rmtree(path, ignore_errors=True)


def write_parquet(df: DataFrame, path: str, partition_by: list[str] | None = None,
                  mode: str = "overwrite", files_per_partition: int | None = 1) -> None:
    """Escritura Parquet con control de archivos chicos.

    `files_per_partition=1` hace repartition por las columnas de partición: un archivo por
    partición de salida, que es lo correcto para tablas de este tamaño (R-5).
    """
    if partition_by and files_per_partition:
        df = df.repartition(*partition_by)
    elif files_per_partition:
        df = df.coalesce(files_per_partition)
    writer = df.write.mode(mode).option("compression", "snappy")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(path)


def record_metrics(cfg: Config, stage: str, **metrics) -> dict:
    """Agrega una línea JSON a _meta/run_metrics.jsonl: la observabilidad mínima del pipeline."""
    cfg.meta.mkdir(parents=True, exist_ok=True)
    entry = {"run_id": RUN_ID, "stage": stage, "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
             **metrics}
    with open(cfg.meta / "run_metrics.jsonl", "a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, default=str) + "\n")
    get_logger(stage).info("métricas %s", json.dumps(metrics, default=str))
    return entry


def dir_stats(path: str | Path) -> dict:
    """Cantidad de archivos Parquet y bytes de una tabla: evidencia de tamaños y small files."""
    p = Path(path)
    files = [f for f in p.rglob("*.parquet")] if p.exists() else []
    return {"path": str(p), "parquet_files": len(files), "bytes": sum(f.stat().st_size for f in files)}
