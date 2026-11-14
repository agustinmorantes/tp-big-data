# Gobierno de datos

Cómo manejamos la calidad, los metadatos, el linaje, los responsables, la seguridad y la
observabilidad en la segunda entrega. Todo lo que está acá se puede verificar en el código o en
`evidence/segunda-entrega/`. Es una versión preliminar y la completamos para la entrega final.

## Controles de calidad

Las reglas se aplican al pasar de Bronze a Silver (ADR-004): Bronze guarda todo y Silver solo lo
que pasa las reglas. Cada registro que no pasa va a `quarantine/<tabla>` con el motivo y la fila
original (ADR-015).

| Tabla | Regla | Qué pasa con la fila | Motivo |
|---|---|---|---|
| usage_events | JSON corrupto | Se rechaza | `corrupt_record` |
| usage_events | `event_id` nulo | Se rechaza | `event_id_null` |
| usage_events | `event_id` repetido en el batch o ya cargado | Se rechaza | `duplicate_event_id_*` |
| usage_events | `timestamp` que no se puede parsear | Se rechaza | `event_ts_invalid` |
| usage_events | `org_id` nulo o que no existe en `customers_orgs` | Se rechaza | `org_id_null` / `orphan_org` |
| usage_events | `cost_usd_increment` nulo | Se rechaza | `cost_null` |
| usage_events | Servicio, región o métrica que no está en el catálogo | Se rechaza | `unknown_*` |
| usage_events | `value` que no es número | Se rechaza | `value_not_numeric` |
| usage_events | `unit` que no corresponde a la métrica | Se rechaza | `unit_metric_mismatch` |
| usage_events | `unit` nulo con `value` presente | Se corrige: la unidad se toma del catálogo de métricas | `unit_imputed_from_metric` |
| usage_events | `cost_usd_increment < -0.01` | Entra a Silver con el flag `is_negative_cost` | - |
| billing_monthly | Subtotal negativo con `taxes/|subtotal|` cerca de 0,21 | Se corrige el signo | `subtotal_sign_corrected` |
| billing_monthly | Subtotal negativo sin ese ratio | Se rechaza | `negative_subtotal_unexplained` |
| billing_monthly | Factura en USD con tasa distinta de 1 | Se corrige (tasa = 1) | `usd_fx_forced_to_1` |
| billing_monthly | `credits` nulo | Se corrige (0) | `credits_null_as_zero` |
| billing_monthly | Moneda distinta de USD, ARS o EUR, o tasa inválida | Se rechaza | `unknown_currency` / `fx_invalid` |
| billing_monthly | `invoice_id` u `org_id` nulos | Se rechaza | `key_null` |
| Maestros | Clave primaria nula | Se rechaza | `<clave>_null` (por ejemplo `user_id_null`) |
| Maestros salvo customers_orgs | `org_id` nulo o inexistente | Se rechaza | `orphan_or_null_org` |
| customers_orgs, resources | Región o servicio que no está en el catálogo | Se rechaza | `unknown_region` / `unknown_service` |
| support_tickets | Severidad desconocida, resuelto antes de creado, CSAT fuera de 0 a 7 | Se rechaza | `unknown_severity` / `resolved_before_created` / `csat_out_of_range` |
| nps_surveys | Fecha nula o NPS fuera de −100 a 100 | Se rechaza | `survey_date_null` / `nps_out_of_range` |
| Todas | Clave primaria duplicada en Bronze | Se queda la primera | `bronze_batch.dedupe_by_pk` |

Los conteos y ejemplos reales están en `evidence/segunda-entrega/quarantine_samples.md`. Las
reglas tienen tests en `tests/test_quality.py` y `tests/test_stream_e2e.py`.

## Metadatos técnicos

- Por fila, en Bronze y Silver: `ingest_ts` (cuándo se cargó), `source_file` (de qué archivo de
  Landing viene) y `load_id` (en qué corrida se cargó). Silver de eventos suma `batch_id`
  (micro-batch) e `is_late`.
- Por corrida: `_meta/run_metrics.jsonl` tiene una línea JSON por etapa y por micro-batch, con el
  `run_id`, los conteos de entrada y salida, los rechazos por motivo y los archivos y bytes
  escritos.
- Por tabla: `evidence/segunda-entrega/lake_inventory.md` (filas, archivos, tamaño y
  particiones), y los esquemas en `gold_samples.md` y `docs/diccionario-datos.md`.

## Linaje

```
landing/<fuente>.csv --bronze_batch--> bronze/<fuente> --silver_masters--> silver/<fuente>
                                                       \-----------------> quarantine/<fuente>
landing/usage_events_stream/*.jsonl --bronze_stream--> bronze/usage_events
        --silver_events (+ silver/customers_orgs, silver/resources)--> silver/usage_events
                                                       \-----------------> quarantine/usage_events
silver/* --gold--> gold/<mart> --serving--> tabla en Cassandra/AstraDB
```

Para una fila de Silver, `source_file` y `load_id` dicen de qué archivo y de qué corrida viene.
Con `event_id` (o la clave del maestro) se encuentra su registro en quarantine si la fila fue
corregida. Qué mart de Gold carga cada tabla de Cassandra está en `LOADS`, en
`src/cpa/serving.py`.

## Responsables

| Área | Responsable | Respaldo |
|---|---|---|
| Ingesta batch y streaming a Bronze | Olivia Garcia | Agustín Morantes |
| Silver, features y marts de Gold | Abril Lombardo | Olivia Garcia |
| Reglas de calidad, quarantine, serving e infraestructura | Agustín Morantes | Abril Lombardo |

El responsable aprueba los cambios de reglas o de esquema de su área. Todo cambio entra por Pull
Request y lo revisa otro integrante.

## Seguridad y acceso

- Credenciales: nunca van al repo. AstraDB se configura con `config/astra.env`, que está en el
  `.gitignore` (hay una plantilla en `config/astra.env.example`), o con variables de entorno. El
  secure connect bundle también está ignorado. El token debería tener el rol mínimo que permita
  escribir en el keyspace.
- Datos personales: `users.email` no pasa a Silver. Lo reemplazamos por `email_sha256` (hash del
  email normalizado) y `email_domain`. Ninguna tabla de Gold ni de Cassandra tiene datos
  personales. Bronze conserva el email original, así que el acceso a Bronze queda limitado al
  equipo de ingeniería.
- Los recursos con el tag `pii:true` se marcan con `has_pii_tag` en Silver.
- Acceso por zona, pensado para producción: Landing y Bronze solo para ingeniería, Silver de
  lectura para analistas, y Gold y Cassandra para consumo. En el MVP local no hay control de
  acceso real.

## Retención y reinicio

- Landing no se modifica nunca; ningún proceso escribe ahí.
- `python -m cpa.run_pipeline reset` borra todo lo derivado (Bronze, Silver, Gold, quarantine,
  checkpoints y métricas) y deja Landing como estaba. Todo se puede regenerar desde Landing.
- Las tablas de Cassandra se recargan pisando las filas existentes. Para vaciarlas hay que hacer
  `TRUNCATE` desde cqlsh o desde la consola de Astra.

## Observabilidad

Cada corrida deja un log en `logs/pipeline_<run_id>.log` (copiado a
`evidence/segunda-entrega/pipeline_log.txt`) y métricas en `_meta/run_metrics.jsonl`. El
progreso de las queries de streaming (`recentProgress`) está resumido en
`evidence/segunda-entrega/streaming.md`. Si una etapa falla, el proceso termina con código 1 y
el error queda en el log.
