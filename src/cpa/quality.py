"""Reglas de calidad como funciones puras sobre DataFrames (testeables sin E/S).

Tres tipos de resultado por fila, nunca un dropna() global (R-3):
- **rechazo**: la fila no puede entrar a Silver -> quarantine con `disposition = 'rejected'`.
- **remediación**: la fila se corrige con una regla determinística, entra a Silver con un flag
  y se guarda una copia del original en quarantine con `disposition = 'remediated'`.
- **flag**: la fila entra a Silver tal cual, marcada (p. ej. costo negativo).
"""
from __future__ import annotations

from typing import Iterable

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from cpa.schemas import CORRUPT, METRIC_UNITS, REGIONS, SERVICES

REJECTED, REMEDIATED = "rejected", "remediated"


def reasons_column(rules: Iterable[tuple[Column, str]]) -> Column:
    """Array con el nombre de cada regla que la fila incumple (vacío si cumple todas)."""
    tagged = [F.when(cond, F.lit(name)) for cond, name in rules]
    return F.filter(F.array(*tagged), lambda x: x.isNotNull())


def _in(col: str, values: Iterable[str]) -> Column:
    return F.col(col).isin(list(values))


# ---------------------------------------------------------------------------------------------
# Eventos de uso
# ---------------------------------------------------------------------------------------------
def expected_unit() -> Column:
    """Unidad esperada según el catálogo de métricas (null si la métrica es desconocida)."""
    return F.create_map(*[F.lit(x) for kv in METRIC_UNITS.items() for x in kv])[F.col("metric")]


def apply_event_rules(df: DataFrame, cost_min: float, org_known_col: str | None = None) -> DataFrame:
    """Agrega `reject_reasons`, `remediations`, `value_num`, `unit_imputed` e `is_negative_cost`.

    Reglas de calidad de eventos: R1 event_id no nulo (y único, ver `split_duplicates`),
    R2 cost_usd_increment >= -0.01 con flag de anomalía, R3 unit no nulo cuando existe value.
    Más controles defensivos: registro corrupto, timestamp no parseable, vocabularios y org huérfana.
    """
    value_num = F.expr("try_cast(value AS DOUBLE)")
    df = df.withColumn("value_num", value_num).withColumn("_expected_unit", expected_unit())

    rules = [
        (F.col(CORRUPT).isNotNull(), "corrupt_record"),
        (F.col("event_id").isNull(), "event_id_null"),                                    # R1
        (F.col("event_ts").isNull(), "event_ts_invalid"),
        (F.col("org_id").isNull(), "org_id_null"),
        (F.col("cost_usd_increment").isNull(), "cost_null"),
        (~_in("service", SERVICES) | F.col("service").isNull(), "unknown_service"),
        (~_in("region", REGIONS) | F.col("region").isNull(), "unknown_region"),
        (F.col("_expected_unit").isNull(), "unknown_metric"),
        (F.col("value").isNotNull() & F.col("value_num").isNull(), "value_not_numeric"),
        # R3 sin remedio posible: hay unit pero no es la de la métrica.
        (F.col("unit").isNotNull() & F.col("_expected_unit").isNotNull()
         & (F.col("unit") != F.col("_expected_unit")), "unit_metric_mismatch"),
    ]
    if org_known_col:
        rules.append((F.col("org_id").isNotNull() & ~F.coalesce(F.col(org_known_col), F.lit(False)), "orphan_org"))

    # R3 con remedio: value presente y unit nula -> la unidad se completa del catálogo de métricas
    # (relación 1:1 verificada en el 100 % de las filas que sí traen unit).
    needs_unit = F.col("value_num").isNotNull() & F.col("unit").isNull() & F.col("_expected_unit").isNotNull()

    return (
        df.withColumn("unit_raw", F.col("unit"))
        .withColumn("reject_reasons", reasons_column(rules))
        .withColumn("remediations", reasons_column([(needs_unit, "unit_imputed_from_metric")]))
        .withColumn("unit_imputed", needs_unit)
        .withColumn("unit", F.when(needs_unit, F.col("_expected_unit")).otherwise(F.col("unit")))
        .withColumn("is_negative_cost", F.coalesce(F.col("cost_usd_increment") < F.lit(cost_min), F.lit(False)))  # R2
        .drop("_expected_unit")
    )


def split_duplicates(df: DataFrame, existing_ids: DataFrame | None) -> tuple[DataFrame, DataFrame]:
    """R1 (unicidad). Devuelve (únicos y nuevos, duplicados).

    Duplicado = mismo event_id dentro del batch (se queda el primero en llegar) o event_id ya
    presente en Silver (reproceso). Esto es lo que hace idempotente a Silver aunque se pierda el
    checkpoint: re-leer todo Bronze no agrega filas (ADR-008).
    """
    w = Window.partitionBy("event_id").orderBy("ingest_ts", "source_file")
    ranked = df.withColumn("_rn", F.row_number().over(w))
    in_batch_dups = ranked.filter("_rn > 1").drop("_rn").withColumn("_dup_reason", F.lit("duplicate_event_id_in_batch"))
    first = ranked.filter("_rn = 1").drop("_rn")
    if existing_ids is None:
        return first, in_batch_dups
    existing = existing_ids.select("event_id").distinct()
    new = first.join(existing, "event_id", "left_anti")
    reprocessed = (first.join(existing, "event_id", "left_semi")
                   .withColumn("_dup_reason", F.lit("duplicate_event_id_already_in_silver")))
    return new, in_batch_dups.unionByName(reprocessed)


# ---------------------------------------------------------------------------------------------
# Billing (ADR-006, ADR-007, S-1)
# ---------------------------------------------------------------------------------------------
def apply_billing_rules(df: DataFrame, tax_rate: float, tolerance: float) -> DataFrame:
    ratio = F.col("taxes") / F.abs(F.col("subtotal"))
    sign_fixable = (F.col("subtotal") < 0) & (F.abs(ratio - F.lit(tax_rate)) <= F.lit(tolerance))
    rules = [
        (F.col(CORRUPT).isNotNull(), "corrupt_record"),
        (F.col("invoice_id").isNull() | F.col("org_id").isNull(), "key_null"),
        (F.col("month").isNull(), "month_null"),
        (F.col("subtotal").isNull(), "subtotal_null"),
        (~_in("currency", ("USD", "ARS", "EUR")) | F.col("currency").isNull(), "unknown_currency"),
        ((F.col("currency") != "USD") & (F.col("exchange_rate_to_usd").isNull()
                                         | (F.col("exchange_rate_to_usd") <= 0)), "fx_invalid"),
        ((F.col("subtotal") < 0) & ~sign_fixable, "negative_subtotal_unexplained"),
    ]
    fx_forced = (F.col("currency") == "USD") & ~F.coalesce(F.col("exchange_rate_to_usd") == 1, F.lit(False))
    remediations = [
        (sign_fixable, "subtotal_sign_corrected"),
        (fx_forced, "usd_fx_forced_to_1"),
        (F.col("credits").isNull(), "credits_null_as_zero"),
    ]
    return (
        df.withColumn("reject_reasons", reasons_column(rules))
        .withColumn("remediations", reasons_column(remediations))
        .withColumn("sign_corrected", sign_fixable)
        .withColumn("fx_forced", F.coalesce(fx_forced, F.lit(False)))
        .withColumn("credits_imputed", F.col("credits").isNull())
        .withColumn("subtotal", F.when(sign_fixable, -F.col("subtotal")).otherwise(F.col("subtotal")))
        .withColumn("credits", F.coalesce("credits", F.lit(0.0)))
        .withColumn("fx_rate_applied", F.when(F.col("currency") == "USD", F.lit(1.0))
                    .otherwise(F.col("exchange_rate_to_usd")))
    )


def generic_rules(df: DataFrame, rules: list[tuple[Column, str]]) -> DataFrame:
    return (df.withColumn("reject_reasons", reasons_column([(F.col(CORRUPT).isNotNull(), "corrupt_record"), *rules]))
            .withColumn("remediations", F.array().cast("array<string>")))


# ---------------------------------------------------------------------------------------------
# Quarantine
# ---------------------------------------------------------------------------------------------
def to_quarantine(df: DataFrame, table: str, disposition: str, reasons_col: str = "reject_reasons") -> DataFrame:
    """Forma común de quarantine: la fila original serializada + motivo y disposición.

    Serializar la fila a JSON permite una única tabla de quarantine para todas las fuentes sin
    pelear con esquemas distintos, y conserva el valor original (p. ej. el `value` no numérico).
    """
    payload_cols = [c for c in df.columns if c not in ("reject_reasons", "remediations")]
    return df.select(
        F.lit(table).alias("source_table"),
        F.col(reasons_col).alias("reasons"),
        F.col(reasons_col)[0].alias("primary_reason"),
        F.lit(disposition).alias("disposition"),
        F.to_json(F.struct(*payload_cols)).alias("record_json"),
        F.current_timestamp().alias("detected_ts"),
    )
