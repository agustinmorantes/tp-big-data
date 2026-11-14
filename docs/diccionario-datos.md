# Diccionario de datos

Esquemas de las tablas tal como quedan en el lake después de correr `run_pipeline all`. Los
tipos son los de Spark. Hay muestras de cada tabla en `evidence/segunda-entrega/gold_samples.md`
y los tamaños y particiones están en `lake_inventory.md`.

## Columnas técnicas (Bronze y Silver)

| Columna | Tipo | Descripción |
|---|---|---|
| `ingest_ts` | timestamp | Momento de ingestión a Bronze (UTC) |
| `source_file` | string | Ruta del archivo de Landing de donde salió la fila |
| `load_id` | string | Id de la corrida (`RUN_ID` = `YYYYmmddTHHMMSSZ-xxxxxx`) que la cargó |
| `_corrupt_record` | string | Solo Bronze: la línea original si no se pudo parsear (si no, nulo) |
| `ingest_date` | date | Partición de Bronze para maestros y eventos (fecha de llegada) |
| `remediations` | array\<string\> | Solo Silver: correcciones aplicadas a la fila (vacío si ninguna) |

## Bronze

Mismo grano y columnas que la fuente, con tipos explícitos (`src/cpa/schemas.py`) más las
columnas técnicas. Partición: `ingest_date` para maestros y eventos, `billing_month` (billing),
`created_month` (tickets) y `touch_month` (marketing).

`bronze/usage_events` guarda `value` y `timestamp` como string. El cast lo hacemos en Silver,
así si el valor no es válido podemos mandar el original a quarantine. `event_ts` es el `timestamp`
ya parseado. `carbon_kg` y `genai_tokens` son nulos en eventos v1.

## Silver

### `silver/usage_events`

Grano: un evento (`event_id` único). Partición: `event_date`.

| Columna | Tipo | Descripción |
|---|---|---|
| `event_id` | string | Clave natural del evento; única en la tabla |
| `event_ts` / `event_date` | timestamp / date | Momento del evento (UTC) y su fecha |
| `org_id`, `org_name`, `industry`, `plan_tier`, `is_enterprise` | | Organización (join con `silver/customers_orgs`) |
| `resource_id`, `resource_state` | string | Recurso y su estado (join con `silver/resources`) |
| `service`, `region`, `metric` | string | Validados contra catálogo |
| `value_num` | double | `value` casteado a número |
| `unit` | string | Unidad; si venía nula se imputa desde la métrica |
| `unit_imputed` | boolean | La unidad fue imputada |
| `cost_usd` | double | `cost_usd_increment` de la fuente (puede ser negativo) |
| `is_negative_cost` | boolean | `cost_usd < -0.01`: ajuste/crédito, se conserva con flag |
| `schema_version` | int | 1 o 2 (v2 desde el 18/07/2025) |
| `carbon_kg`, `genai_tokens` | double, bigint | Solo v2 (nulos en v1); tokens solo en `genai` |
| `requests`, `cpu_hours`, `storage_gb_hours` | double | `value_num` pivotado según `metric` (nulo si no aplica) |
| `is_late` | boolean | `event_ts` < máximo `event_ts` de batches anteriores − 6 h (ADR-011) |
| `batch_id` | int | Micro-batch de Structured Streaming que lo publicó |

### Maestros

| Tabla | Grano / clave | Cambios respecto de Bronze |
|---|---|---|
| `customers_orgs` | `org_id` | Sin `nps_score` (ADR-005) |
| `users` | `user_id` | `email` → `email_sha256` + `email_domain` (PII) |
| `resources` | `resource_id` | `tags` (array parseado de `tags_json`), `has_pii_tag` |
| `billing_monthly` | `invoice_id` · partición `billing_month` | `subtotal` con signo corregido, `credits` nulo → 0, `fx_rate_applied` (USD = 1), `subtotal_usd`, `credits_usd`, `taxes_usd`, `revenue_usd = (subtotal − credits + taxes) × fx`, flags `sign_corrected`, `fx_forced`, `credits_imputed` |
| `support_tickets` | `ticket_id` · partición `created_month` | `is_open` (sin `resolved_at`), `resolution_days`, `is_critical` |
| `nps_surveys` | (`org_id`, `survey_date`) | — |
| `marketing_touches` | `touch_id` · partición `touch_month` | — |

Los maestros particionados por `ingest_date` se toman de la última carga de Bronze.

## Quarantine (`quarantine/<tabla>`)

| Columna | Tipo | Descripción |
|---|---|---|
| `source_table` | string | Tabla de origen |
| `reasons` | array\<string\> | Todos los motivos (rechazo o remediación) |
| `primary_reason` | string | Primer motivo, para agrupar |
| `disposition` | string | `rejected` (no está en Silver) o `remediated` (está corregida en Silver) |
| `record_json` | string | La fila tal como llegó, serializada |
| `detected_ts` | timestamp | Momento de detección |
| `batch_id`, `batch_key` | int, string | Solo eventos: micro-batch y partición (arranque de la query + batch) |

## Gold

Todos los marts se recalculan enteros desde Silver en cada corrida. Los montos están en USD.

### `org_daily_usage_by_service` (FinOps)

Grano: `org_id`, `usage_date`, `service`. Partición: `usage_date`.

| Columna | Descripción |
|---|---|
| `daily_cost_usd` | Costo neto del día (suma incluyendo ajustes negativos) |
| `gross_cost_usd` | Costo sin los eventos `is_negative_cost` |
| `requests`, `cpu_hours`, `storage_gb_hours` | Suma del uso por métrica |
| `genai_tokens`, `carbon_kg` | Suma (solo eventos v2) |
| `carbon_coverage` | Fracción de eventos del día con `carbon_kg` (0 antes del 18/07) |
| `events`, `negative_cost_events`, `late_events` | Conteos de eventos |

### `cost_anomaly_mart` (FinOps, analítica)

Grano: `org_id`, `usage_date`, `service`.

`daily_cost_usd`, `median_cost_usd` y `mad_cost_usd` de la serie (org, servicio), `robust_z`
(MAD), `classic_z` (media/desvío, para comparar), `history_days`, `is_anomaly`
(`|robust_z| > 3.5` y `history_days ≥ 14`) y `anomaly_type` (`spike` / `drop`).

### `top_services_14d` (FinOps)

Grano: `org_id`, `as_of_date`, `service`.

Para cada fecha de corte: `cost_usd_window` y `requests_window` de los 14 días que terminan en
`as_of_date`, `active_days` y `cost_rank` (1 = mayor costo).

### `revenue_by_org_month` (FinOps)

Grano: `org_id`, `month`.

`invoices`, `subtotal_usd`, `credits_usd`, `taxes_usd`, `revenue_usd`, `currency`,
`fx_rate_applied` y los flags de remediación de billing.

### `tickets_by_org_date` (Soporte)

Grano: `org_id`, `ticket_date`, `severity`.

`tickets`, `sla_breached`, `sla_breach_rate`, `csat_avg` (escala 0–7, sin nulos),
`csat_responses`, `open_tickets`, `avg_resolution_days`. `ticket_date` = fecha de creación.

### `tickets_by_severity_date` (Soporte)

Grano: `severity`, `ticket_date`.

Igual que el anterior pero sin `org_id`, y con `orgs_affected`. Es el que usa la consulta 3
(tickets críticos por día).

### `genai_tokens_by_org_date` (Producto, GenAI)

Grano: `org_id`, `usage_date`.

Solo el servicio `genai`: `genai_tokens`, `genai_cost_usd`, `genai_events`, `events_with_tokens`,
`token_coverage` (fracción de eventos con tokens) y `cost_per_1k_tokens_usd`.

## Serving (Cassandra/AstraDB)

| Tabla | Mart de origen | Clave primaria | Consulta |
|---|---|---|---|
| `org_daily_usage_by_service` | `org_daily_usage_by_service` | `((org_id), usage_date, service)` | Q1 |
| `top_services_by_org_14d` | `top_services_14d` | `((org_id, as_of_date), cost_rank)` | Q2 |
| `tickets_by_severity_date` | `tickets_by_severity_date` | `((severity), ticket_date)` | Q3 |
| `tickets_by_org_date` | `tickets_by_org_date` | `((org_id), ticket_date, severity)` | Q3 por organización |
| `revenue_by_org_month` | `revenue_by_org_month` | `((org_id), month)` | Q4 |
| `genai_tokens_by_org_date` | `genai_tokens_by_org_date` | `((org_id), usage_date)` | Q5 |
| `cost_anomalies_by_org` | `cost_anomaly_mart` (solo `is_anomaly`) | `((org_id), usage_date, service)` | Q6 |

El DDL completo está en `cql/01_tables.cql`.
