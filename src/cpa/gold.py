"""Silver -> Gold: marts de negocio con grano explícito.

| Mart                         | Grano                          | Dominio        |
|------------------------------|--------------------------------|----------------|
| org_daily_usage_by_service   | org_id, usage_date, service    | FinOps         |
| cost_anomaly_mart            | org_id, usage_date, service    | FinOps / ML    |
| top_services_14d             | org_id, as_of_date, service    | FinOps         |
| revenue_by_org_month         | org_id, month                  | FinOps         |
| tickets_by_org_date          | org_id, ticket_date, severity  | Soporte        |
| genai_tokens_by_org_date     | org_id, usage_date             | Producto/GenAI |

Cada mart se recalcula completo desde Silver y se escribe con overwrite: es determinístico, así
que re-ejecutar no duplica, y los eventos tardíos que entraron a Silver quedan incorporados.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from cpa.common import dir_stats, get_logger, record_metrics, write_parquet
from cpa.config import Config

log = get_logger("gold")


# --- FinOps -------------------------------------------------------------------------------------
def org_daily_usage_by_service(events: DataFrame) -> DataFrame:
    return (
        events.groupBy("org_id", F.col("event_date").alias("usage_date"), "service")
        .agg(
            F.first("org_name", ignorenulls=True).alias("org_name"),
            F.round(F.sum("cost_usd"), 4).alias("daily_cost_usd"),
            F.round(F.sum(F.when(~F.col("is_negative_cost"), F.col("cost_usd"))), 4).alias("gross_cost_usd"),
            F.round(F.sum("requests"), 2).alias("requests"),
            F.round(F.sum("cpu_hours"), 4).alias("cpu_hours"),
            F.round(F.sum("storage_gb_hours"), 4).alias("storage_gb_hours"),
            F.sum("genai_tokens").alias("genai_tokens"),
            F.round(F.sum("carbon_kg"), 6).alias("carbon_kg"),
            F.count("*").alias("events"),
            F.sum(F.col("is_negative_cost").cast("int")).alias("negative_cost_events"),
            F.sum(F.col("is_late").cast("int")).alias("late_events"),
            # carbon_kg solo existe en v2: se informa la cobertura para no leer un nulo como cero.
            F.round(F.avg(F.col("carbon_kg").isNotNull().cast("double")), 4).alias("carbon_coverage"),
        )
    )


def cost_anomaly_mart(daily: DataFrame, threshold: float, min_days: int) -> DataFrame:
    """Anomalías de costo diario por (org, servicio) con z-score robusto (MAD).

    El costo tiene cola pesada (máximo 19× el p99): media y desvío quedan dominados por los
    mismos spikes que se quieren detectar, mediana y MAD no (ADR-013).
    z_robusto = 0.6745 * (x - mediana) / MAD; anomalía si |z| > umbral (3.5, Iglewicz-Hoaglin).
    Se informa también el z clásico para comparar.
    """
    key = ["org_id", "service"]
    med = daily.groupBy(*key).agg(
        F.percentile_approx("daily_cost_usd", 0.5, 10000).alias("median_cost"),
        F.avg("daily_cost_usd").alias("mean_cost"),
        F.stddev_pop("daily_cost_usd").alias("std_cost"),
        F.count("*").alias("history_days"),
    )
    with_med = daily.join(med, key)
    mad = (with_med.withColumn("abs_dev", F.abs(F.col("daily_cost_usd") - F.col("median_cost")))
           .groupBy(*key).agg(F.percentile_approx("abs_dev", 0.5, 10000).alias("mad_cost")))
    scored = (
        with_med.join(mad, key)
        .withColumn("robust_z", F.when(F.col("mad_cost") > 0, F.lit(0.6745) * (F.col("daily_cost_usd") - F.col("median_cost")) / F.col("mad_cost")))
        .withColumn("classic_z", F.when(F.col("std_cost") > 0, (F.col("daily_cost_usd") - F.col("mean_cost")) / F.col("std_cost")))
        .withColumn("scored", F.col("history_days") >= min_days)
        .withColumn("is_anomaly", F.col("scored") & (F.abs(F.col("robust_z")) > threshold))
        .withColumn("anomaly_type", F.when(~F.col("is_anomaly"), None)
                    .when(F.col("robust_z") > 0, "spike").otherwise("drop"))
    )
    return scored.select(
        "org_id", "usage_date", "service", "org_name", "daily_cost_usd", "events", "negative_cost_events",
        F.round("median_cost", 4).alias("median_cost_usd"), F.round("mad_cost", 4).alias("mad_cost_usd"),
        F.round("robust_z", 3).alias("robust_z"), F.round("classic_z", 3).alias("classic_z"),
        "history_days", "is_anomaly", "anomaly_type",
    )


def top_services_rolling(daily: DataFrame, window_days: int) -> DataFrame:
    """Costo acumulado por servicio en los últimos N días, para cada fecha de corte.

    Cassandra no ordena por un agregado: el top-N se precalcula acá y la tabla de serving lo
    guarda ya ordenado por costo (query-first, ADR-012).
    """
    dates = daily.select(F.col("usage_date").alias("as_of_date")).distinct()
    pairs = daily.select("org_id", "service").distinct()
    grid = pairs.crossJoin(dates)
    joined = grid.join(daily.select("org_id", "service", "usage_date", "daily_cost_usd", "requests"), ["org_id", "service"])
    window = joined.filter((F.col("usage_date") <= F.col("as_of_date"))
                           & (F.col("usage_date") > F.date_sub("as_of_date", window_days)))
    agg = window.groupBy("org_id", "as_of_date", "service").agg(
        F.round(F.sum("daily_cost_usd"), 4).alias("cost_usd_window"),
        F.round(F.sum("requests"), 2).alias("requests_window"),
        F.countDistinct("usage_date").alias("active_days"),
    )
    rank = Window.partitionBy("org_id", "as_of_date").orderBy(F.col("cost_usd_window").desc(), "service")
    return agg.withColumn("cost_rank", F.row_number().over(rank)).withColumn("window_days", F.lit(window_days))


def revenue_by_org_month(billing: DataFrame) -> DataFrame:
    return billing.select(
        "org_id", F.col("month"), "invoice_id", "currency",
        F.col("fx_rate_applied"), "subtotal", "credits", "taxes",
        "subtotal_usd", "credits_usd", "taxes_usd", "revenue_usd",
        "sign_corrected", "fx_forced", "credits_imputed",
    ).groupBy("org_id", "month").agg(
        F.first("currency").alias("currency"),
        F.first("fx_rate_applied").alias("fx_rate_applied"),
        F.count("*").alias("invoices"),
        F.round(F.sum("subtotal_usd"), 2).alias("subtotal_usd"),
        F.round(F.sum("credits_usd"), 2).alias("credits_usd"),
        F.round(F.sum("taxes_usd"), 2).alias("taxes_usd"),
        F.round(F.sum("revenue_usd"), 2).alias("revenue_usd"),
        F.max(F.col("sign_corrected").cast("int")).cast("boolean").alias("sign_corrected"),
        F.max(F.col("fx_forced").cast("int")).cast("boolean").alias("fx_forced"),
        F.max(F.col("credits_imputed").cast("int")).cast("boolean").alias("credits_imputed"),
    )


# --- Soporte ------------------------------------------------------------------------------------
def tickets_by_org_date(tickets: DataFrame) -> DataFrame:
    return tickets.groupBy("org_id", F.col("created_at").alias("ticket_date"), "severity").agg(
        F.count("*").alias("tickets"),
        F.sum(F.col("sla_breached").cast("int")).alias("sla_breached"),
        F.round(F.avg(F.col("sla_breached").cast("double")), 4).alias("sla_breach_rate"),
        F.round(F.avg("csat"), 3).alias("csat_avg"),
        F.count("csat").alias("csat_responses"),
        F.sum(F.col("is_open").cast("int")).alias("open_tickets"),
        F.round(F.avg("resolution_days"), 2).alias("avg_resolution_days"),
    )


def tickets_by_severity_date(tickets: DataFrame) -> DataFrame:
    """Vista global por severidad y día (consulta 3: evolución de críticos y SLA breach)."""
    return tickets.groupBy("severity", F.col("created_at").alias("ticket_date")).agg(
        F.count("*").alias("tickets"),
        F.sum(F.col("sla_breached").cast("int")).alias("sla_breached"),
        F.round(F.avg(F.col("sla_breached").cast("double")), 4).alias("sla_breach_rate"),
        F.countDistinct("org_id").alias("orgs_affected"),
        F.round(F.avg("csat"), 3).alias("csat_avg"),
    )


# --- Producto / GenAI ---------------------------------------------------------------------------
def genai_tokens_by_org_date(events: DataFrame) -> DataFrame:
    genai = events.filter(F.col("service") == "genai")
    return genai.groupBy("org_id", F.col("event_date").alias("usage_date")).agg(
        F.sum("genai_tokens").alias("genai_tokens"),
        F.round(F.sum("cost_usd"), 4).alias("genai_cost_usd"),
        F.count("*").alias("genai_events"),
        F.sum(F.col("genai_tokens").isNotNull().cast("int")).alias("events_with_tokens"),
    ).withColumn(
        "cost_per_1k_tokens_usd",
        F.when(F.col("genai_tokens") > 0, F.round(F.col("genai_cost_usd") / F.col("genai_tokens") * 1000, 6)),
    ).withColumn("token_coverage", F.round(F.col("events_with_tokens") / F.col("genai_events"), 4))


# --- orquestación ------------------------------------------------------------------------------
def run(spark: SparkSession, cfg: Config) -> list[dict]:
    a, g = cfg.section("anomaly"), cfg.section("gold")
    events = spark.read.parquet(cfg.zone("silver", "usage_events"))
    daily = org_daily_usage_by_service(events).cache()

    marts: list[tuple[str, DataFrame, list[str] | None]] = [
        ("org_daily_usage_by_service", daily, ["usage_date"]),
        ("cost_anomaly_mart", cost_anomaly_mart(daily, a["robust_z_threshold"], a["min_history_days"]), None),
        ("top_services_14d", top_services_rolling(daily, g["top_services_window_days"]), None),
        ("revenue_by_org_month", revenue_by_org_month(spark.read.parquet(cfg.zone("silver", "billing_monthly"))), None),
        ("tickets_by_org_date", tickets_by_org_date(spark.read.parquet(cfg.zone("silver", "support_tickets"))), None),
        ("tickets_by_severity_date", tickets_by_severity_date(spark.read.parquet(cfg.zone("silver", "support_tickets"))), None),
        ("genai_tokens_by_org_date", genai_tokens_by_org_date(events), None),
    ]
    results = []
    for name, df, parts in marts:
        path = cfg.zone("gold", name)
        # Overwrite completo (no dinámico): un mart es una foto derivada de Silver, no un log.
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
        write_parquet(df, path, partition_by=parts)
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        stats = {"mart": name, "rows": spark.read.parquet(path).count(), **dir_stats(path)}
        log.info("%-28s rows=%-6d files=%d", name, stats["rows"], stats["parquet_files"])
        results.append(stats)
    daily.unpersist()
    record_metrics(cfg, "gold", marts=results)
    return results
