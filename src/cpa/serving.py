"""Gold -> Cassandra/AstraDB.

La carga es desde Spark: cada partición del DataFrame abre una sesión con el driver de Python
y ejecuta INSERTs preparados en paralelo (foreachPartition). En Cassandra un INSERT es un
upsert por clave primaria, así que re-cargar el mismo mart pisa las mismas filas (idempotencia).

Se eligió el driver en lugar del spark-cassandra-connector porque funciona igual contra
Cassandra local y AstraDB (secure connect bundle) sin depender de un JAR atado a la versión
de Spark de Colab (ADR-012).
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from cpa.common import get_logger, record_metrics
from cpa.config import Config

log = get_logger("serving")
CQL_DIR = Path(__file__).resolve().parents[2] / "cql"

# tabla Cassandra -> (mart Gold, filtro opcional, columnas con su tipo Spark de destino)
LOADS: dict[str, tuple[str, str | None, dict[str, str]]] = {
    "org_daily_usage_by_service": ("org_daily_usage_by_service", None, {
        "org_id": "string", "usage_date": "date", "service": "string", "org_name": "string",
        "daily_cost_usd": "double", "gross_cost_usd": "double", "requests": "double", "cpu_hours": "double",
        "storage_gb_hours": "double", "genai_tokens": "long", "carbon_kg": "double", "carbon_coverage": "double",
        "events": "long", "negative_cost_events": "long", "late_events": "long"}),
    "top_services_by_org_14d": ("top_services_14d", None, {
        "org_id": "string", "as_of_date": "date", "cost_rank": "int", "service": "string",
        "cost_usd_window": "double", "requests_window": "double", "active_days": "long", "window_days": "int"}),
    "tickets_by_severity_date": ("tickets_by_severity_date", None, {
        "severity": "string", "ticket_date": "date", "tickets": "long", "sla_breached": "long",
        "sla_breach_rate": "double", "orgs_affected": "long", "csat_avg": "double"}),
    "tickets_by_org_date": ("tickets_by_org_date", None, {
        "org_id": "string", "ticket_date": "date", "severity": "string", "tickets": "long",
        "sla_breached": "long", "sla_breach_rate": "double", "csat_avg": "double", "csat_responses": "long",
        "open_tickets": "long", "avg_resolution_days": "double"}),
    "revenue_by_org_month": ("revenue_by_org_month", None, {
        "org_id": "string", "month": "date", "currency": "string", "fx_rate_applied": "double",
        "invoices": "long", "subtotal_usd": "double", "credits_usd": "double", "taxes_usd": "double",
        "revenue_usd": "double", "sign_corrected": "boolean", "fx_forced": "boolean", "credits_imputed": "boolean"}),
    "genai_tokens_by_org_date": ("genai_tokens_by_org_date", None, {
        "org_id": "string", "usage_date": "date", "genai_tokens": "long", "genai_cost_usd": "double",
        "genai_events": "long", "events_with_tokens": "long", "token_coverage": "double",
        "cost_per_1k_tokens_usd": "double"}),
    "cost_anomalies_by_org": ("cost_anomaly_mart", "is_anomaly", {
        "org_id": "string", "usage_date": "date", "service": "string", "org_name": "string",
        "daily_cost_usd": "double", "median_cost_usd": "double", "mad_cost_usd": "double",
        "robust_z": "double", "classic_z": "double", "anomaly_type": "string"}),
}


def connect(settings: dict):
    """Devuelve (cluster, session) contra Cassandra local o AstraDB según `serving.target`."""
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster

    if settings["target"] == "astra":
        if not settings.get("astra_bundle") or not settings.get("astra_token"):
            raise RuntimeError("Faltan ASTRA_SECURE_CONNECT_BUNDLE / ASTRA_DB_APPLICATION_TOKEN (config/astra.env)")
        cluster = Cluster(cloud={"secure_connect_bundle": settings["astra_bundle"]},
                          auth_provider=PlainTextAuthProvider("token", settings["astra_token"]),
                          connect_timeout=30)
    else:
        cluster = Cluster([settings["cassandra_host"]], port=settings["cassandra_port"], connect_timeout=30)
    session = cluster.connect()
    session.default_timeout = 60
    return cluster, session


def _statements(path: Path) -> list[str]:
    lines = [ln for ln in path.read_text(encoding="utf-8").splitlines() if not ln.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def apply_schema(settings: dict) -> list[str]:
    cluster, session = connect(settings)
    try:
        if settings["target"] != "astra":
            for stmt in _statements(CQL_DIR / "00_keyspace_local.cql"):
                session.execute(stmt.replace("cloud_provider_analytics", settings["keyspace"]))
        session.set_keyspace(settings["keyspace"])
        created = []
        for stmt in _statements(CQL_DIR / "01_tables.cql"):
            session.execute(stmt)
            created.append(stmt.split("(")[0].split()[-1])
        log.info("schema aplicado en %s: %s", settings["keyspace"], created)
        return created
    finally:
        cluster.shutdown()


def _write_partition(settings: dict, table: str, columns: list[str]):
    def write(rows):
        from cassandra.concurrent import execute_concurrent_with_args

        rows = [tuple(r[c] for c in columns) for r in rows]
        if not rows:
            return
        cluster, session = connect(settings)
        try:
            session.set_keyspace(settings["keyspace"])
            stmt = session.prepare(f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join('?' * len(columns))})")
            results = execute_concurrent_with_args(session, stmt, rows, concurrency=64, raise_on_first_error=True)
            failed = [r for r in results if not r.success]
            if failed:
                raise RuntimeError(f"{len(failed)} inserts fallidos en {table}")
        finally:
            cluster.shutdown()
    return write


def prepare(df: DataFrame, filter_col: str | None, columns: dict[str, str]) -> DataFrame:
    if filter_col:
        df = df.filter(F.col(filter_col))
    return df.select(*[F.col(c).cast(t).alias(c) for c, t in columns.items()])


def count_rows(settings: dict, tables: list[str]) -> dict[str, int]:
    cluster, session = connect(settings)
    try:
        session.set_keyspace(settings["keyspace"])
        return {t: session.execute(f"SELECT COUNT(*) FROM {t}").one()[0] for t in tables}
    finally:
        cluster.shutdown()


def run(spark: SparkSession, cfg: Config, tables: list[str] | None = None) -> dict:
    settings = cfg.serving
    parts = int(settings.get("write_partitions", 4))
    apply_schema(settings)
    tables = tables or list(LOADS)
    before = count_rows(settings, tables)
    loaded = {}
    for table in tables:
        mart, filter_col, columns = LOADS[table]
        df = prepare(spark.read.parquet(cfg.zone("gold", mart)), filter_col, columns)
        n = df.count()
        df.repartition(parts).foreachPartition(_write_partition(settings, table, list(columns)))
        loaded[table] = n
        log.info("%-28s -> %d filas enviadas", table, n)
    after = count_rows(settings, tables)
    stats = {"target": settings["target"], "keyspace": settings["keyspace"], "rows_sent": loaded,
             "rows_before": before, "rows_after": after}
    record_metrics(cfg, "serving", **stats)
    return stats
