"""Punto de entrada: `python -m cpa.run_pipeline <etapa> [<etapa> ...]`.

Etapas, en el orden de `all`:
  bronze-batch     Landing CSV -> Bronze (7 fuentes)
  silver-masters   Bronze -> Silver de maestros + quarantine
  bronze-stream    Landing JSONL -> Bronze con Structured Streaming (checkpoint)
  silver-events    Bronze -> Silver de eventos en streaming (foreachBatch, dedupe, late data)
  compact          compacta Silver de eventos a un archivo por fecha
  gold             Silver -> marts Gold
  serving          Gold -> Cassandra/AstraDB (schema + upsert)
  queries          consultas CQL con salida a evidence/

Otras: speed-view (watermark nativo), evidence, idempotency, reset.
"""
from __future__ import annotations

import argparse
import sys
import time

from cpa import bronze_batch, bronze_stream, evidence, gold, queries, serving, silver_events, silver_masters
from cpa.common import get_logger, get_spark, record_metrics, remove_path, setup_logging
from cpa.config import load_config

log = get_logger("run")

PIPELINE = ["bronze-batch", "silver-masters", "bronze-stream", "silver-events", "compact", "gold", "serving",
            "queries"]


def reset(cfg) -> None:
    """Borra todo lo derivado (Bronze..Gold, quarantine, checkpoints, métricas). Landing no se toca."""
    for sub in ("bronze", "silver", "gold", "quarantine", "_checkpoints", "_meta"):
        remove_path(cfg.lake / sub)
    log.info("lake reiniciado (landing intacto): %s", cfg.lake)


def idempotency(spark, cfg) -> dict:
    """Re-ejecuta el pipeline sobre el mismo input y compara conteos antes/después.

    Escenario 1: re-ejecución normal (checkpoints intactos).
    Escenario 2: se borra el checkpoint de Silver de eventos y se reprocesa todo Bronze: el
    dedupe por event_id contra Silver tiene que dejar los conteos iguales.
    """
    targets = list(serving.LOADS)
    before = {"lake": evidence.table_counts(spark, cfg), "cassandra": serving.count_rows(cfg.serving, targets)}
    for stage in ["bronze-batch", "silver-masters", "bronze-stream", "silver-events", "gold", "serving"]:
        STAGES[stage](spark, cfg)
    after_rerun = {"lake": evidence.table_counts(spark, cfg), "cassandra": serving.count_rows(cfg.serving, targets)}

    remove_path(cfg.checkpoint("silver_usage_events"))
    reprocess = silver_events.run(spark, cfg)
    STAGES["gold"](spark, cfg)
    STAGES["serving"](spark, cfg)
    after_reprocess = {"lake": evidence.table_counts(spark, cfg), "cassandra": serving.count_rows(cfg.serving, targets)}

    rows = []
    for kind in ("lake", "cassandra"):
        for t in sorted(before[kind]):
            b, a1, a2 = before[kind][t], after_rerun[kind].get(t), after_reprocess[kind].get(t)
            rows.append((kind, t, b, a1, a2))
    # Quarantine de eventos crece a propósito en el escenario 2: registra los duplicados detectados.
    md = ["# Idempotencia: conteos antes / después", "",
          "1. **Antes**: estado tras la corrida completa del pipeline.",
          "2. **Re-ejecución**: se corren de nuevo todas las etapas con los checkpoints intactos.",
          "3. **Reproceso**: se borra el checkpoint de `silver_usage_events` y se relee todo Bronze.", "",
          "| Destino | Tabla | Antes | Re-ejecución | Reproceso sin checkpoint | ¿Igual? |", "|---|---|---:|---:|---:|:---:|"]
    for kind, t, b, a1, a2 in rows:
        same = "sí" if b == a1 == a2 else ("crece (esperado)" if t == "quarantine/usage_events" else "no")
        md.append(f"| {kind} | `{t}` | {b} | {a1} | {a2} | {same} |")
    md += ["", "En el reproceso, `quarantine/usage_events` suma los event_id ya publicados que se "
               "detectaron como duplicados (`duplicate_event_id_already_in_silver`): es la evidencia de que "
               "el dedupe actuó, no un duplicado en Silver."]
    out = cfg.evidence / "idempotencia.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(md) + "\n", encoding="utf-8")
    broken = [r for r in rows if not (r[2] == r[3] == r[4]) and r[1] != "quarantine/usage_events"]
    result = {"tables_checked": len(rows), "not_idempotent": [r[1] for r in broken],
              "silver_reprocess": reprocess}
    record_metrics(cfg, "idempotency", **result)
    if broken:
        raise RuntimeError(f"Conteos distintos tras re-ejecutar: {broken}")
    return result


STAGES = {
    "bronze-batch": lambda spark, cfg: bronze_batch.run(spark, cfg),
    "silver-masters": lambda spark, cfg: silver_masters.run(spark, cfg),
    "bronze-stream": lambda spark, cfg: bronze_stream.run(spark, cfg),
    "silver-events": lambda spark, cfg: silver_events.run(spark, cfg),
    "compact": lambda spark, cfg: silver_events.compact(spark, cfg),
    "speed-view": lambda spark, cfg: bronze_stream.run_speed_view(spark, cfg),
    "gold": lambda spark, cfg: gold.run(spark, cfg),
    "serving": lambda spark, cfg: serving.run(spark, cfg),
    "queries": lambda spark, cfg: queries.run(spark, cfg),
    "idempotency": idempotency,
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("stages", nargs="+", choices=[*STAGES, "all", "evidence", "reset"])
    parser.add_argument("--config", help="ruta a pipeline.toml (default config/pipeline.toml)")
    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    log_file = setup_logging(cfg)
    stages = [s for st in args.stages for s in (PIPELINE if st == "all" else [st])]
    if "reset" in stages:
        reset(cfg)
        stages = [s for s in stages if s != "reset"]
    if not stages:
        return 0

    spark = get_spark(cfg)
    try:
        for stage in stages:
            t0 = time.time()
            log.info("==> %s", stage)
            if stage == "evidence":
                evidence.run(spark, cfg, log_file)
            else:
                STAGES[stage](spark, cfg)
            elapsed = round(time.time() - t0, 1)
            record_metrics(cfg, "stage_done", name=stage, seconds=elapsed)
    except Exception:
        log.exception("el pipeline falló")
        return 1
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
