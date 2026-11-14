"""Esquemas explícitos de las 8 fuentes (nada de inferSchema) y vocabularios conformados.

Cada esquema de Landing termina en `_corrupt_record`: con mode=PERMISSIVE las filas que no
parsean no rompen la carga, quedan con el texto crudo en esa columna y Silver las manda a
quarantine (ADR-004).
"""
from __future__ import annotations

from dataclasses import dataclass

CORRUPT = "_corrupt_record"


@dataclass(frozen=True)
class BatchSource:
    name: str
    file: str
    ddl: str                 # esquema DDL de Landing
    primary_key: tuple[str, ...]
    partition_col: str       # columna de partición en Bronze
    partition_expr: str      # expresión SQL que la calcula


# Maestros: snapshot completo por carga -> partición por fecha de ingesta.
# Hechos batch: partición por mes del hecho (billing, tickets, marketing) -> pocas particiones,
# un archivo por partición y re-ejecutar pisa el mismo mes (ADR-003, ADR-010).
BATCH_SOURCES: dict[str, BatchSource] = {
    s.name: s
    for s in [
        BatchSource(
            "customers_orgs", "customers_orgs.csv",
            "org_id STRING, org_name STRING, industry STRING, hq_region STRING, plan_tier STRING, "
            "is_enterprise BOOLEAN, signup_date DATE, sales_rep STRING, lifecycle_stage STRING, "
            "marketing_source STRING, nps_score DOUBLE",
            ("org_id",), "ingest_date", "to_date(ingest_ts)"),
        BatchSource(
            "users", "users.csv",
            "user_id STRING, org_id STRING, email STRING, role STRING, active BOOLEAN, "
            "created_at DATE, last_login DATE",
            ("user_id",), "ingest_date", "to_date(ingest_ts)"),
        BatchSource(
            "resources", "resources.csv",
            "resource_id STRING, org_id STRING, service STRING, region STRING, created_at DATE, "
            "state STRING, tags_json STRING",
            ("resource_id",), "ingest_date", "to_date(ingest_ts)"),
        BatchSource(
            "billing_monthly", "billing_monthly.csv",
            "invoice_id STRING, org_id STRING, month DATE, subtotal DOUBLE, credits DOUBLE, "
            "taxes DOUBLE, currency STRING, exchange_rate_to_usd DOUBLE",
            ("invoice_id",), "billing_month", "date_format(month, 'yyyy-MM')"),
        BatchSource(
            "support_tickets", "support_tickets.csv",
            "ticket_id STRING, org_id STRING, category STRING, severity STRING, created_at DATE, "
            "resolved_at DATE, csat DOUBLE, sla_breached BOOLEAN",
            ("ticket_id",), "created_month", "date_format(created_at, 'yyyy-MM')"),
        BatchSource(
            "marketing_touches", "marketing_touches.csv",
            "touch_id STRING, org_id STRING, campaign STRING, channel STRING, timestamp DATE, "
            "clicked BOOLEAN, converted BOOLEAN",
            ("touch_id",), "touch_month", "date_format(timestamp, 'yyyy-MM')"),
        BatchSource(
            "nps_surveys", "nps_surveys.csv",
            "org_id STRING, survey_date DATE, nps_score DOUBLE, comment STRING",
            ("org_id", "survey_date"), "ingest_date", "to_date(ingest_ts)"),
    ]
}

# Eventos de uso (JSONL). `value` se lee como STRING a propósito: llega a veces como número y a
# veces como texto; castearlo en la lectura lo convertiría a nulo en silencio (R-10).
# `timestamp` también se lee como texto y se parsea aparte, para poder separar los no parseables.
# carbon_kg y genai_tokens son nullable: solo existen en schema_version=2 (R-2).
USAGE_EVENTS_DDL = (
    "event_id STRING, timestamp STRING, org_id STRING, resource_id STRING, service STRING, "
    "region STRING, metric STRING, value STRING, unit STRING, cost_usd_increment DOUBLE, "
    "schema_version INT, carbon_kg DOUBLE, genai_tokens DOUBLE, " + f"{CORRUPT} STRING"
)

# Vocabularios conformados (perfilado de la primera entrega: coinciden entre fuentes).
# Silver los usa como control defensivo: un valor desconocido va a quarantine.
SERVICES = ("compute", "storage", "database", "networking", "analytics", "genai")
REGIONS = ("us-east", "us-west", "eu-west", "eu-central", "sa-east", "ap-south", "ap-northeast")
SEVERITIES = ("low", "medium", "high", "critical")
CURRENCIES = ("USD", "ARS", "EUR")

# Catálogo de métricas: cada métrica tiene una única unidad válida. Es lo que permite completar
# `unit` cuando falta (regla R4) sin adivinar.
METRIC_UNITS = {"requests": "count", "cpu_hours": "hours", "storage_gb_hours": "gb_hours"}


def with_corrupt(ddl: str) -> str:
    return f"{ddl}, {CORRUPT} STRING"
