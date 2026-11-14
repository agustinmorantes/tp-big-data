# Inventario del lake

Generado por `python -m cpa.run_pipeline evidence`.

| Tabla | Filas | Archivos Parquet | Tamaño (KB) | Particiones |
|---|---:|---:|---:|---|
| `bronze/billing_monthly` | 240 | 3 | 21.8 | billing_month (3) |
| `bronze/customers_orgs` | 80 | 1 | 7.3 | ingest_date (1) |
| `bronze/marketing_touches` | 1500 | 4 | 36.5 | touch_month (4) |
| `bronze/nps_surveys` | 92 | 1 | 4.3 | ingest_date (1) |
| `bronze/resources` | 400 | 1 | 12.1 | ingest_date (1) |
| `bronze/support_tickets` | 1000 | 4 | 33.6 | created_month (4) |
| `bronze/usage_events` | 43200 | 120 | 3301.8 | ingest_date (1) |
| `bronze/users` | 800 | 1 | 23.8 | ingest_date (1) |
| `silver/billing_monthly` | 240 | 3 | 34.9 | billing_month (3) |
| `silver/customers_orgs` | 80 | 1 | 6.8 | — |
| `silver/marketing_touches` | 1500 | 4 | 36.9 | touch_month (4) |
| `silver/nps_surveys` | 92 | 1 | 4.4 | — |
| `silver/resources` | 400 | 1 | 13.5 | — |
| `silver/support_tickets` | 1000 | 4 | 38.1 | created_month (4) |
| `silver/usage_events` | 43200 | 60 | 2844.7 | event_date (60) |
| `silver/users` | 800 | 1 | 69.2 | — |
| `gold/cost_anomaly_mart` | 11050 | 1 | 156.0 | — |
| `gold/genai_tokens_by_org_date` | 1131 | 1 | 22.8 | — |
| `gold/org_daily_usage_by_service` | 11050 | 60 | 773.4 | usage_date (60) |
| `gold/revenue_by_org_month` | 240 | 1 | 9.6 | — |
| `gold/tickets_by_org_date` | 984 | 1 | 8.6 | — |
| `gold/tickets_by_severity_date` | 364 | 1 | 4.4 | — |
| `gold/top_services_14d` | 15599 | 1 | 142.8 | — |
| `quarantine/billing_monthly` | 208 | 1 | 23.1 | — |
| `quarantine/customers_orgs` | 0 | 1 | 0.9 | — |
| `quarantine/marketing_touches` | 0 | 1 | 0.9 | — |
| `quarantine/nps_surveys` | 0 | 1 | 0.9 | — |
| `quarantine/resources` | 0 | 1 | 0.9 | — |
| `quarantine/support_tickets` | 0 | 1 | 0.9 | — |
| `quarantine/usage_events` | 45238 | 24 | 5038.8 | batch_key (24) |
| `quarantine/users` | 0 | 1 | 0.9 | — |

Ruta base: `data/datalake/`