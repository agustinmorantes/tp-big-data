"""Bronze -> Silver para las 7 fuentes batch: conformado, reglas de calidad y quarantine.

Cada tabla sale de la última foto de Bronze (máximo `ingest_date` en los maestros), pasa por sus
reglas y se escribe completa con `overwrite`: re-ejecutar produce exactamente la misma salida.
"""
from __future__ import annotations

from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from cpa.common import dir_stats, get_logger, record_metrics, write_parquet
from cpa.config import Config
from cpa.quality import REJECTED, REMEDIATED, apply_billing_rules, generic_rules, to_quarantine
from cpa.schemas import BATCH_SOURCES, CORRUPT, REGIONS, SERVICES, SEVERITIES

log = get_logger("silver_masters")
TECH_COLS = ["ingest_ts", "source_file", "load_id"]


def latest_bronze(spark: SparkSession, cfg: Config, table: str) -> DataFrame:
    df = spark.read.parquet(cfg.zone("bronze", table))
    if BATCH_SOURCES[table].partition_col == "ingest_date":
        last = df.agg(F.max("ingest_date")).first()[0]
        df = df.filter(F.col("ingest_date") == F.lit(last)).drop("ingest_date")
    return df


def orphan_rule(df: DataFrame, orgs: DataFrame) -> tuple[DataFrame, tuple]:
    known = orgs.select("org_id").withColumn("_org_known", F.lit(True))
    df = df.join(F.broadcast(known), "org_id", "left")
    return df, (F.col("org_id").isNull() | F.col("_org_known").isNull(), "orphan_or_null_org")


# --- transformaciones por tabla ---------------------------------------------------------------
def conform_customers(df: DataFrame, _orgs: DataFrame | None) -> DataFrame:
    # nps_score del maestro no se promueve (ADR-005): la fuente de verdad es nps_surveys.
    df = generic_rules(df.drop("nps_score"), [
        (F.col("org_id").isNull(), "org_id_null"),
        (~F.col("hq_region").isin(list(REGIONS)), "unknown_region"),
    ])
    return df


def conform_users(df: DataFrame, orgs: DataFrame) -> DataFrame:
    df, orphan = orphan_rule(df, orgs)
    # PII (R-9): el email no sale de Bronze. Queda el hash (para joins/conteos) y el dominio.
    df = (df.withColumn("email_sha256", F.sha2(F.lower(F.trim("email")), 256))
            .withColumn("email_domain", F.lower(F.substring_index("email", "@", -1))))
    df = generic_rules(df, [(F.col("user_id").isNull(), "user_id_null"), orphan])
    return df.drop("email")


def conform_resources(df: DataFrame, orgs: DataFrame) -> DataFrame:
    df, orphan = orphan_rule(df, orgs)
    df = df.withColumn("tags", F.coalesce(F.from_json("tags_json", "array<string>"), F.array().cast("array<string>")))
    df = df.withColumn("has_pii_tag", F.array_contains("tags", "pii:true"))
    return generic_rules(df, [
        (F.col("resource_id").isNull(), "resource_id_null"),
        (~F.col("service").isin(list(SERVICES)), "unknown_service"),
        (~F.col("region").isin(list(REGIONS)), "unknown_region"),
        orphan,
    ])


def conform_billing(df: DataFrame, orgs: DataFrame, cfg: Config) -> DataFrame:
    q = cfg.section("quality")
    df, orphan = orphan_rule(df, orgs)
    df = apply_billing_rules(df, q["billing_tax_rate"], q["billing_tax_tolerance"])
    df = df.withColumn("reject_reasons", F.when(orphan[0], F.array_union("reject_reasons", F.array(F.lit(orphan[1]))))
                       .otherwise(F.col("reject_reasons")))
    fx = F.col("fx_rate_applied")
    return (df.withColumn("subtotal_usd", F.round(F.col("subtotal") * fx, 2))
              .withColumn("credits_usd", F.round(F.col("credits") * fx, 2))
              .withColumn("taxes_usd", F.round(F.col("taxes") * fx, 2))
              .withColumn("revenue_usd", F.round((F.col("subtotal") - F.col("credits") + F.col("taxes")) * fx, 2)))


def conform_tickets(df: DataFrame, orgs: DataFrame) -> DataFrame:
    df, orphan = orphan_rule(df, orgs)
    df = (df.withColumn("is_open", F.col("resolved_at").isNull())   # S-2: nulo = ticket abierto
            .withColumn("resolution_days", F.datediff("resolved_at", "created_at"))
            .withColumn("is_critical", F.col("severity") == "critical"))
    return generic_rules(df, [
        (F.col("ticket_id").isNull(), "ticket_id_null"),
        (~F.col("severity").isin(list(SEVERITIES)), "unknown_severity"),
        (F.col("resolved_at") < F.col("created_at"), "resolved_before_created"),
        # D-2 abierta: CSAT observado 0-7; fuera de ese rango es inválido seguro.
        (F.col("csat").isNotNull() & ((F.col("csat") < 0) | (F.col("csat") > 7)), "csat_out_of_range"),
        orphan,
    ])


def conform_nps(df: DataFrame, orgs: DataFrame) -> DataFrame:
    df, orphan = orphan_rule(df, orgs)
    return generic_rules(df, [
        (F.col("survey_date").isNull(), "survey_date_null"),
        (F.col("nps_score").isNotNull() & ((F.col("nps_score") < -100) | (F.col("nps_score") > 100)),
         "nps_out_of_range"),
        orphan,
    ])


def conform_marketing(df: DataFrame, orgs: DataFrame) -> DataFrame:
    df, orphan = orphan_rule(df, orgs)
    return generic_rules(df, [(F.col("touch_id").isNull(), "touch_id_null"), orphan])


# --- orquestación ------------------------------------------------------------------------------
def publish(cfg: Config, table: str, conformed: DataFrame, partition_by: list[str] | None) -> dict:
    conformed = conformed.cache()
    rejected = conformed.filter(F.size("reject_reasons") > 0)
    accepted = conformed.filter(F.size("reject_reasons") == 0)
    remediated = accepted.filter(F.size("remediations") > 0)

    quarantine = to_quarantine(rejected, table, REJECTED).unionByName(
        to_quarantine(remediated, table, REMEDIATED, reasons_col="remediations"))
    silver = accepted.drop("reject_reasons", CORRUPT, "_org_known")

    silver_path = cfg.zone("silver", table)
    write_parquet(silver, silver_path, partition_by=partition_by)
    write_parquet(quarantine, cfg.zone("quarantine", table))
    stats = {"table": table, "input": conformed.count(), "silver": accepted.count(),
             "rejected": rejected.count(), "remediated": remediated.count(),
             "reject_reasons": {r["r"]: r["n"] for r in rejected.select(F.explode("reject_reasons").alias("r"))
                                .groupBy("r").agg(F.count("*").alias("n")).collect()},
             "remediations": {r["r"]: r["n"] for r in remediated.select(F.explode("remediations").alias("r"))
                              .groupBy("r").agg(F.count("*").alias("n")).collect()},
             "files": dir_stats(silver_path)["parquet_files"]}
    conformed.unpersist()
    log.info("%-18s silver=%-5d rejected=%-3d remediated=%d", table, stats["silver"], stats["rejected"],
             stats["remediated"])
    return stats


def run(spark: SparkSession, cfg: Config) -> list[dict]:
    customers = conform_customers(latest_bronze(spark, cfg, "customers_orgs"), None)
    results = [publish(cfg, "customers_orgs", customers, None)]
    orgs = spark.read.parquet(cfg.zone("silver", "customers_orgs")).select("org_id").cache()

    plan: list[tuple[str, Callable[[DataFrame], DataFrame], list[str] | None]] = [
        ("users", lambda d: conform_users(d, orgs), None),
        ("resources", lambda d: conform_resources(d, orgs), None),
        ("billing_monthly", lambda d: conform_billing(d, orgs, cfg), ["billing_month"]),
        ("support_tickets", lambda d: conform_tickets(d, orgs), ["created_month"]),
        ("nps_surveys", lambda d: conform_nps(d, orgs), None),
        ("marketing_touches", lambda d: conform_marketing(d, orgs), ["touch_month"]),
    ]
    for table, fn, parts in plan:
        results.append(publish(cfg, table, fn(latest_bronze(spark, cfg, table)), parts))
    record_metrics(cfg, "silver_masters", tables=results)
    return results
