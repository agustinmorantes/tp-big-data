"""Consultas de negocio sobre Cassandra/AstraDB con salida reproducible.

Cada consulta se ejecuta con CQL parametrizado y se guarda en evidence/.../queries/ con el CQL
exacto (valores incluidos), el motivo del modelo y las filas devueltas.

El dataset es histórico (jul-ago 2025): "últimos N días" se mide contra la última fecha con
datos, no contra hoy.
"""
from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from cpa.common import get_logger, record_metrics
from cpa.config import Config
from cpa.serving import connect

log = get_logger("queries")


def demo_parameters(spark: SparkSession, cfg: Config, org_id: str | None = None) -> dict:
    """Parámetros por defecto: la org con mayor costo total y la última fecha de cada fuente."""
    daily = spark.read.parquet(cfg.zone("gold", "org_daily_usage_by_service"))
    if org_id is None:
        org_id = (daily.groupBy("org_id").agg(F.sum("daily_cost_usd").alias("c"))
                  .orderBy(F.desc("c"), "org_id").first()["org_id"])
    last_usage = daily.agg(F.max("usage_date")).first()[0]
    last_ticket = spark.read.parquet(cfg.zone("gold", "tickets_by_severity_date")).agg(F.max("ticket_date")).first()[0]
    return {"org_id": org_id, "last_usage_date": last_usage, "last_ticket_date": last_ticket}


def build_queries(p: dict) -> list[dict]:
    end: date = p["last_usage_date"]
    t_end: date = p["last_ticket_date"]
    org = p["org_id"]
    return [
        {"id": "q1", "title": "Costos y requests diarios por organización y servicio en un rango de fechas",
         "why": "Partición = org_id, clustering (usage_date DESC, service): el rango de fechas es un "
                "slice contiguo de una sola partición.",
         "cql": "SELECT usage_date, service, daily_cost_usd, requests, events, negative_cost_events "
                "FROM org_daily_usage_by_service WHERE org_id = %s AND usage_date >= %s AND usage_date <= %s",
         "params": (org, end - timedelta(days=6), end)},
        {"id": "q2", "title": "Top-5 servicios por costo acumulado en los últimos 14 días para una organización",
         "why": "El ranking se precalcula en Gold por fecha de corte; partición (org_id, as_of_date) "
                "ordenada por cost_rank: la consulta es un LIMIT sobre una partición.",
         "cql": "SELECT cost_rank, service, cost_usd_window, requests_window, active_days "
                "FROM top_services_by_org_14d WHERE org_id = %s AND as_of_date = %s LIMIT 5",
         "params": (org, end)},
        {"id": "q3", "title": "Evolución diaria de tickets críticos y tasa de SLA breach (últimos 30 días)",
         "why": "Partición = severity, clustering ticket_date DESC: una partición, un rango.",
         "cql": "SELECT ticket_date, tickets, sla_breached, sla_breach_rate, orgs_affected "
                "FROM tickets_by_severity_date WHERE severity = %s AND ticket_date > %s",
         "params": ("critical", t_end - timedelta(days=30))},
        {"id": "q4", "title": "Revenue mensual con créditos e impuestos, normalizado a USD",
         "why": "Partición = org_id, clustering month DESC: el historial de facturación de una org.",
         "cql": "SELECT month, currency, fx_rate_applied, subtotal_usd, credits_usd, taxes_usd, revenue_usd, "
                "sign_corrected, fx_forced FROM revenue_by_org_month WHERE org_id = %s",
         "params": (org,)},
        {"id": "q5", "title": "Tokens GenAI y costo estimado por día",
         "why": "Partición = org_id, clustering usage_date DESC. Solo hay tokens desde schema v2 "
                "(18/07/2025): token_coverage lo hace explícito.",
         "cql": "SELECT usage_date, genai_tokens, genai_cost_usd, cost_per_1k_tokens_usd, token_coverage "
                "FROM genai_tokens_by_org_date WHERE org_id = %s AND usage_date >= %s",
         "params": (org, end - timedelta(days=13))},
        {"id": "q6", "title": "Anomalías de costo detectadas (MAD) para una organización",
         "why": "Componente analítico: solo los días marcados, partición = org_id.",
         "cql": "SELECT usage_date, service, daily_cost_usd, median_cost_usd, robust_z, anomaly_type "
                "FROM cost_anomalies_by_org WHERE org_id = %s",
         "params": (org,)},
    ]


def _literal(v) -> str:
    return f"'{v}'" if isinstance(v, (str, date)) else str(v)


def _markdown_table(columns: list[str], rows: list) -> str:
    def fmt(v):
        return f"{v:.4f}" if isinstance(v, float) else ("" if v is None else str(v))
    lines = ["| " + " | ".join(columns) + " |", "|" + "---|" * len(columns)]
    lines += ["| " + " | ".join(fmt(v) for v in row) + " |" for row in rows]
    return "\n".join(lines)


def run(spark: SparkSession, cfg: Config, org_id: str | None = None) -> list[dict]:
    settings = cfg.serving
    params = demo_parameters(spark, cfg, org_id)
    out_dir = cfg.evidence / "queries"
    out_dir.mkdir(parents=True, exist_ok=True)
    cluster, session = connect(settings)
    results = []
    try:
        session.set_keyspace(settings["keyspace"])
        for q in build_queries(params):
            rs = session.execute(q["cql"], q["params"])
            rows = list(rs)
            columns = list(rs.column_names or [])
            executed = q["cql"] % tuple(_literal(v) for v in q["params"])
            md = (f"# {q['id'].upper()} — {q['title']}\n\n"
                  f"Destino: `{settings['target']}` · keyspace `{settings['keyspace']}`\n\n"
                  f"**Modelo query-first:** {q['why']}\n\n```sql\n{executed};\n```\n\n"
                  f"**Filas devueltas:** {len(rows)}\n\n{_markdown_table(columns, rows)}\n")
            (out_dir / f"{q['id']}.md").write_text(md, encoding="utf-8")
            log.info("%s -> %d filas", q["id"], len(rows))
            print(f"\n{q['id'].upper()} — {q['title']}\n{executed};\n{_markdown_table(columns, rows[:15])}")
            results.append({"id": q["id"], "rows": len(rows), "cql": executed})
    finally:
        cluster.shutdown()
    record_metrics(cfg, "queries", params={k: str(v) for k, v in params.items()}, results=results)
    return results
