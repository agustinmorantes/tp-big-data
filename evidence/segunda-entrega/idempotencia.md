# Idempotencia: conteos antes / después

1. **Antes**: estado tras la corrida completa del pipeline.
2. **Re-ejecución**: se corren de nuevo todas las etapas con los checkpoints intactos.
3. **Reproceso**: se borra el checkpoint de `silver_usage_events` y se relee todo Bronze.

| Destino | Tabla | Antes | Re-ejecución | Reproceso sin checkpoint | ¿Igual? |
|---|---|---:|---:|---:|:---:|
| lake | `bronze/billing_monthly` | 240 | 240 | 240 | sí |
| lake | `bronze/customers_orgs` | 80 | 80 | 80 | sí |
| lake | `bronze/marketing_touches` | 1500 | 1500 | 1500 | sí |
| lake | `bronze/nps_surveys` | 92 | 92 | 92 | sí |
| lake | `bronze/resources` | 400 | 400 | 400 | sí |
| lake | `bronze/support_tickets` | 1000 | 1000 | 1000 | sí |
| lake | `bronze/usage_events` | 43200 | 43200 | 43200 | sí |
| lake | `bronze/users` | 800 | 800 | 800 | sí |
| lake | `gold/cost_anomaly_mart` | 11050 | 11050 | 11050 | sí |
| lake | `gold/genai_tokens_by_org_date` | 1131 | 1131 | 1131 | sí |
| lake | `gold/org_daily_usage_by_service` | 11050 | 11050 | 11050 | sí |
| lake | `gold/revenue_by_org_month` | 240 | 240 | 240 | sí |
| lake | `gold/tickets_by_org_date` | 984 | 984 | 984 | sí |
| lake | `gold/tickets_by_severity_date` | 364 | 364 | 364 | sí |
| lake | `gold/top_services_14d` | 15599 | 15599 | 15599 | sí |
| lake | `quarantine/billing_monthly` | 208 | 208 | 208 | sí |
| lake | `quarantine/customers_orgs` | 0 | 0 | 0 | sí |
| lake | `quarantine/marketing_touches` | 0 | 0 | 0 | sí |
| lake | `quarantine/nps_surveys` | 0 | 0 | 0 | sí |
| lake | `quarantine/resources` | 0 | 0 | 0 | sí |
| lake | `quarantine/support_tickets` | 0 | 0 | 0 | sí |
| lake | `quarantine/usage_events` | 2038 | 2038 | 45238 | crece (esperado) |
| lake | `quarantine/users` | 0 | 0 | 0 | sí |
| lake | `silver/billing_monthly` | 240 | 240 | 240 | sí |
| lake | `silver/customers_orgs` | 80 | 80 | 80 | sí |
| lake | `silver/marketing_touches` | 1500 | 1500 | 1500 | sí |
| lake | `silver/nps_surveys` | 92 | 92 | 92 | sí |
| lake | `silver/resources` | 400 | 400 | 400 | sí |
| lake | `silver/support_tickets` | 1000 | 1000 | 1000 | sí |
| lake | `silver/usage_events` | 43200 | 43200 | 43200 | sí |
| lake | `silver/users` | 800 | 800 | 800 | sí |
| cassandra | `cost_anomalies_by_org` | 148 | 148 | 148 | sí |
| cassandra | `genai_tokens_by_org_date` | 1131 | 1131 | 1131 | sí |
| cassandra | `org_daily_usage_by_service` | 11050 | 11050 | 11050 | sí |
| cassandra | `revenue_by_org_month` | 240 | 240 | 240 | sí |
| cassandra | `tickets_by_org_date` | 984 | 984 | 984 | sí |
| cassandra | `tickets_by_severity_date` | 364 | 364 | 364 | sí |
| cassandra | `top_services_by_org_14d` | 15599 | 15599 | 15599 | sí |

En el reproceso, `quarantine/usage_events` suma los event_id ya publicados que se detectaron como duplicados (`duplicate_event_id_already_in_silver`): es la evidencia de que el dedupe actuó, no un duplicado en Silver.
