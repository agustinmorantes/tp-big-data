# Streaming: micro-batches, late data y watermark

Generado desde `_meta/run_metrics.jsonl`. Cada fila es un micro-batch de `silver_usage_events` (foreachBatch), en orden de ejecución. Las corridas posteriores a la primera son las de la prueba de idempotencia: re-ejecución (sin input nuevo) y reproceso sin checkpoint (todo se detecta como duplicado).

Bronze stream (primera corrida): **43200** filas en **12** micro-batches, 120 archivos Parquet.

| run_id | batch | input | → Silver | rechazados | duplicados | remediados | tardíos | costo < −0.01 | max event_ts previo |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 20261113T192404Z-eb3465 | 0 | 3600 | 3600 | 0 | 0 | 165 | 0 | 16 | — |
| 20261113T192404Z-eb3465 | 1 | 3600 | 3600 | 0 | 0 | 196 | 3581 | 20 | 2025-08-31 23:56:00 |
| 20261113T192404Z-eb3465 | 2 | 3600 | 3600 | 0 | 0 | 150 | 3580 | 22 | 2025-08-31 23:56:00 |
| 20261113T192404Z-eb3465 | 3 | 3600 | 3600 | 0 | 0 | 172 | 3585 | 17 | 2025-08-31 23:56:00 |
| 20261113T192404Z-eb3465 | 4 | 3600 | 3600 | 0 | 0 | 157 | 3587 | 16 | 2025-08-31 23:56:00 |
| 20261113T192404Z-eb3465 | 5 | 3600 | 3600 | 0 | 0 | 172 | 3585 | 16 | 2025-08-31 23:56:00 |
| 20261113T192404Z-eb3465 | 6 | 3600 | 3600 | 0 | 0 | 192 | 3580 | 15 | 2025-08-31 23:56:00 |
| 20261113T192404Z-eb3465 | 7 | 3600 | 3600 | 0 | 0 | 170 | 3581 | 23 | 2025-08-31 23:56:00 |
| 20261113T192404Z-eb3465 | 8 | 3600 | 3600 | 0 | 0 | 170 | 3591 | 18 | 2025-08-31 23:56:00 |
| 20261113T192404Z-eb3465 | 9 | 3600 | 3600 | 0 | 0 | 156 | 3583 | 12 | 2025-08-31 23:56:00 |
| 20261113T192404Z-eb3465 | 10 | 3600 | 3600 | 0 | 0 | 175 | 3588 | 23 | 2025-08-31 23:56:00 |
| 20261113T192404Z-eb3465 | 11 | 3600 | 3600 | 0 | 0 | 163 | 3590 | 13 | 2025-08-31 23:58:00 |
| 20261113T192404Z-eb3465 | 0 | 3600 | 0 | 0 | 3600 | 0 | 0 | 0 | — |
| 20261113T192404Z-eb3465 | 1 | 3600 | 0 | 0 | 3600 | 0 | 0 | 0 | — |
| 20261113T192404Z-eb3465 | 2 | 3600 | 0 | 0 | 3600 | 0 | 0 | 0 | — |
| 20261113T192404Z-eb3465 | 3 | 3600 | 0 | 0 | 3600 | 0 | 0 | 0 | — |
| 20261113T192404Z-eb3465 | 4 | 3600 | 0 | 0 | 3600 | 0 | 0 | 0 | — |
| 20261113T192404Z-eb3465 | 5 | 3600 | 0 | 0 | 3600 | 0 | 0 | 0 | — |
| 20261113T192404Z-eb3465 | 6 | 3600 | 0 | 0 | 3600 | 0 | 0 | 0 | — |
| 20261113T192404Z-eb3465 | 7 | 3600 | 0 | 0 | 3600 | 0 | 0 | 0 | — |
| 20261113T192404Z-eb3465 | 8 | 3600 | 0 | 0 | 3600 | 0 | 0 | 0 | — |
| 20261113T192404Z-eb3465 | 9 | 3600 | 0 | 0 | 3600 | 0 | 0 | 0 | — |
| 20261113T192404Z-eb3465 | 10 | 3600 | 0 | 0 | 3600 | 0 | 0 | 0 | — |
| 20261113T192404Z-eb3465 | 11 | 3600 | 0 | 0 | 3600 | 0 | 0 | 0 | — |

## Vista de velocidad con watermark nativo (ADR-011)

Ventanas de 1 h por organización con `withWatermark("event_ts", "10 minutes")`, un archivo de Bronze por trigger:

- Filas de entrada: **35640**
- Descartadas por el watermark: **35543** (99.73 %)
- Ventanas en memoria: **750**

Como cada JSONL cubre los 60 días, tras el primer trigger el watermark avanza al final del período y casi todo lo que sigue llega "tarde". Por eso la ruta principal no usa el watermark nativo para descartar: marca `is_late` y conserva el evento.
