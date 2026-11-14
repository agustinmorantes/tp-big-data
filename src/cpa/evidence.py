"""Genera la evidencia de ejecución en evidence/segunda-entrega/ a partir del lake.

Todo sale de los datos ya escritos (no de valores tipeados a mano): rutas, tamaños, conteos,
muestras de quarantine, muestras de Gold y resumen de anomalías.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from cpa.common import dir_stats, get_logger
from cpa.config import Config

log = get_logger("evidence")
ZONES = ("bronze", "silver", "gold", "quarantine")


def _md_table(df, limit: int = 10, truncate: int = 60) -> str:
    rows = df.limit(limit).collect()
    cols = df.columns
    def fmt(v):
        s = f"{v:.4f}" if isinstance(v, float) else ("" if v is None else str(v))
        s = s.replace("|", "\\|").replace("\n", " ")
        return s if len(s) <= truncate else s[: truncate - 1] + "…"
    out = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    out += ["| " + " | ".join(fmt(r[c]) for c in cols) + " |" for r in rows]
    return "\n".join(out)


def table_counts(spark: SparkSession, cfg: Config) -> dict[str, int]:
    counts = {}
    for zone in ZONES:
        base = Path(cfg.zone(zone))
        if not base.exists():
            continue
        for t in sorted(p for p in base.iterdir() if p.is_dir() and not p.name.startswith(("_", "."))):
            counts[f"{zone}/{t.name}"] = spark.read.parquet(str(t)).count()
    return counts


def lake_inventory(spark: SparkSession, cfg: Config) -> str:
    lines = ["# Inventario del lake", "", "Generado por `python -m cpa.run_pipeline evidence`.", "",
             "| Tabla | Filas | Archivos Parquet | Tamaño (KB) | Particiones |", "|---|---:|---:|---:|---|"]
    counts = table_counts(spark, cfg)
    for name, n in counts.items():
        path = Path(cfg.zone(*name.split("/")))
        st = dir_stats(path)
        parts = sorted({p.name.split("=")[0] for p in path.iterdir() if p.is_dir() and "=" in p.name})
        n_parts = len([p for p in path.iterdir() if p.is_dir() and "=" in p.name])
        part_desc = f"{', '.join(parts)} ({n_parts})" if parts else "—"
        lines.append(f"| `{name}` | {n} | {st['parquet_files']} | {st['bytes'] / 1024:.1f} | {part_desc} |")
    lines += ["", f"Ruta base: `{cfg.lake.relative_to(cfg.root).as_posix()}/`"]
    return "\n".join(lines)


def quarantine_report(spark: SparkSession, cfg: Config) -> str:
    base = Path(cfg.zone("quarantine"))
    cols = ["source_table", "primary_reason", "disposition", "reasons", "record_json"]
    frames = [spark.read.parquet(str(t)).select(*cols) for t in sorted(base.iterdir()) if t.is_dir()] if base.exists() else []
    q = None
    for f in frames:
        q = f if q is None else q.unionByName(f)
    lines = ["# Quarantine: conteos y muestras", "",
             "`rejected` = no entra a Silver. `remediated` = entra corregida con flag; acá queda el original.", ""]
    if q is None:
        return "\n".join(lines + ["Sin quarantine."])
    summary = q.groupBy("source_table", "disposition", "primary_reason").count().orderBy("source_table", "disposition", F.desc("count"))
    lines += ["## Conteo por tabla, disposición y motivo", "", _md_table(summary, 100), ""]
    for r in summary.collect():
        sample = q.filter((F.col("source_table") == r["source_table"]) & (F.col("primary_reason") == r["primary_reason"])
                          & (F.col("disposition") == r["disposition"])).select("reasons", "record_json")
        lines += [f"### {r['source_table']} · {r['disposition']} · `{r['primary_reason']}` ({r['count']} filas)", "",
                  _md_table(sample, 3, truncate=400), ""]
    return "\n".join(lines)


def gold_report(spark: SparkSession, cfg: Config) -> str:
    lines = ["# Muestras de Gold", ""]
    base = Path(cfg.zone("gold"))
    for t in sorted(p.name for p in base.iterdir() if p.is_dir()):
        df = spark.read.parquet(str(base / t))
        order = [c for c in ("org_id", "usage_date", "ticket_date", "month", "service", "severity") if c in df.columns]
        lines += [f"## {t} ({df.count()} filas)", "", "```", df._jdf.schema().treeString().strip(), "```", "",
                  _md_table(df.orderBy(*order) if order else df, 5), ""]
    return "\n".join(lines)


def anomaly_report(spark: SparkSession, cfg: Config) -> str:
    a = spark.read.parquet(cfg.zone("gold", "cost_anomaly_mart"))
    scored = a.filter("history_days >= 0")
    thr = cfg.section("anomaly")["robust_z_threshold"]
    total = scored.count()
    robust = scored.filter("is_anomaly").count()
    classic = scored.filter(F.abs("classic_z") > 3).count()
    both = scored.filter("is_anomaly AND abs(classic_z) > 3").count()
    by_type = scored.filter("is_anomaly").groupBy("anomaly_type").count()
    top = scored.filter("is_anomaly").orderBy(F.desc("robust_z")).select(
        "org_id", "usage_date", "service", "daily_cost_usd", "median_cost_usd", "mad_cost_usd", "robust_z", "classic_z")
    events = spark.read.parquet(cfg.zone("silver", "usage_events"))
    p99 = events.approxQuantile("cost_usd", [0.99], 0.0001)[0]
    spike_days = (events.filter(F.col("cost_usd") > 10 * p99).select("org_id", F.col("event_date").alias("usage_date"), "service")
                  .distinct())
    caught = spike_days.join(scored.filter("is_anomaly"), ["org_id", "usage_date", "service"]).count()
    n_spike_days = spike_days.count()
    return "\n".join([
        "# Componente analítico: anomalías de costo (MAD)", "",
        f"- Días (org, servicio) puntuados: **{total}**",
        f"- Anomalías por z robusto (|z| > {thr}): **{robust}** ({100 * robust / max(total, 1):.2f} %)",
        f"- Anomalías por z clásico (|z| > 3): **{classic}**; coinciden con el robusto: **{both}**",
        f"- Días con al menos un evento > 10× el p99 de costo por evento (p99 = {p99:.2f} USD): **{n_spike_days}**; "
        f"detectados por MAD: **{caught}** ({100 * caught / max(n_spike_days, 1):.1f} %)", "",
        "## Por tipo", "", _md_table(by_type), "",
        "## Top 15 por z robusto", "", _md_table(top, 15), "",
    ])


def streaming_report(cfg: Config) -> str:
    """Micro-batches de Silver de eventos y vista de velocidad, a partir de _meta/run_metrics.jsonl."""
    path = cfg.meta / "run_metrics.jsonl"
    entries = [json.loads(l) for l in path.read_text(encoding="utf-8").splitlines()] if path.exists() else []
    batches = [e for e in entries if e["stage"] == "silver_events_batch"]
    bronze = [e for e in entries if e["stage"] == "bronze_stream"]
    speed = [e for e in entries if e["stage"] == "speed_view"]
    lines = ["# Streaming: micro-batches, late data y watermark", "",
             "Generado desde `_meta/run_metrics.jsonl`. Cada fila es un micro-batch de "
             "`silver_usage_events` (foreachBatch), en orden de ejecución. Las corridas posteriores a la "
             "primera son las de la prueba de idempotencia: re-ejecución (sin input nuevo) y reproceso "
             "sin checkpoint (todo se detecta como duplicado).", ""]
    if bronze:
        b = bronze[0]
        lines += [f"Bronze stream (primera corrida): **{b['rows_ingested']}** filas en "
                  f"**{b['micro_batches_with_data']}** micro-batches, {b['parquet_files']} archivos Parquet.", ""]
    lines += ["| run_id | batch | input | → Silver | rechazados | duplicados | remediados | tardíos | costo < −0.01 | max event_ts previo |",
              "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|"]
    for e in batches:
        lines.append(f"| {e['run_id']} | {e['batch_id']} | {e['input']} | {e['silver_new']} | {e['rejected']} | "
                     f"{e['duplicates']} | {e['remediated']} | {e['late_events']} | {e['negative_cost_flagged']} | "
                     f"{e['max_event_ts_before_batch'] or '—'} |")
    if speed:
        s = speed[-1]
        lines += ["", "## Vista de velocidad con watermark nativo (ADR-011)", "",
                  f"Ventanas de 1 h por organización con `withWatermark(\"event_ts\", \"{s['watermark_delay']}\")`, "
                  "un archivo de Bronze por trigger:", "",
                  f"- Filas de entrada: **{s['input_rows']}**",
                  f"- Descartadas por el watermark: **{s['rows_dropped_by_watermark']}** ({s['dropped_pct']} %)",
                  f"- Ventanas en memoria: **{s['windows_in_memory']}**", "",
                  "Como cada JSONL cubre los 60 días, tras el primer trigger el watermark avanza al final del "
                  "período y casi todo lo que sigue llega \"tarde\". Por eso la ruta principal no usa el "
                  "watermark nativo para descartar: marca `is_late` y conserva el evento."]
    return "\n".join(lines) + "\n"


def run(spark: SparkSession, cfg: Config, log_file: Path | None = None) -> Path:
    out = cfg.evidence
    out.mkdir(parents=True, exist_ok=True)
    (out / "lake_inventory.md").write_text(lake_inventory(spark, cfg), encoding="utf-8")
    (out / "quarantine_samples.md").write_text(quarantine_report(spark, cfg), encoding="utf-8")
    (out / "gold_samples.md").write_text(gold_report(spark, cfg), encoding="utf-8")
    (out / "anomalies.md").write_text(anomaly_report(spark, cfg), encoding="utf-8")
    (out / "streaming.md").write_text(streaming_report(cfg), encoding="utf-8")
    metrics = cfg.meta / "run_metrics.jsonl"
    if metrics.exists():
        shutil.copy(metrics, out / "run_metrics.jsonl")
    if log_file and log_file.exists():
        shutil.copy(log_file, out / "pipeline_log.txt")
    log.info("evidencia escrita en %s", out)
    return out


def write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
