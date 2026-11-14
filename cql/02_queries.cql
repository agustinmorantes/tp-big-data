-- Consultas de negocio con los valores de la demo.
-- org_53lc58dr = la organización con mayor costo total; el dataset termina el 2025-08-31, así que
-- "últimos N días" se mide contra esa fecha. `python -m cpa.run_pipeline queries` ejecuta estas
-- mismas consultas y guarda CQL + resultados en evidence/segunda-entrega/queries/.
-- Uso: cqlsh -k cloud_provider_analytics -f cql/02_queries.cql

-- Q1: costos y requests diarios por organización y servicio en un rango de fechas.
SELECT usage_date, service, daily_cost_usd, requests, events, negative_cost_events
FROM org_daily_usage_by_service
WHERE org_id = 'org_53lc58dr' AND usage_date >= '2025-08-25' AND usage_date <= '2025-08-31';

-- Q2: top-5 servicios por costo acumulado en los últimos 14 días.
SELECT cost_rank, service, cost_usd_window, requests_window, active_days
FROM top_services_by_org_14d
WHERE org_id = 'org_53lc58dr' AND as_of_date = '2025-08-31'
LIMIT 5;

-- Q3: evolución diaria de tickets críticos y tasa de SLA breach, últimos 30 días.
SELECT ticket_date, tickets, sla_breached, sla_breach_rate, orgs_affected
FROM tickets_by_severity_date
WHERE severity = 'critical' AND ticket_date > '2025-08-01';

-- Q3 (variante por organización).
SELECT ticket_date, severity, tickets, sla_breached, sla_breach_rate, csat_avg
FROM tickets_by_org_date
WHERE org_id = 'org_53lc58dr' AND ticket_date > '2025-08-01';

-- Q4: revenue mensual con créditos e impuestos, normalizado a USD.
SELECT month, currency, fx_rate_applied, subtotal_usd, credits_usd, taxes_usd, revenue_usd, sign_corrected, fx_forced
FROM revenue_by_org_month
WHERE org_id = 'org_53lc58dr';

-- Q5: tokens GenAI y costo estimado por día (últimos 14 días).
SELECT usage_date, genai_tokens, genai_cost_usd, cost_per_1k_tokens_usd, token_coverage
FROM genai_tokens_by_org_date
WHERE org_id = 'org_53lc58dr' AND usage_date >= '2025-08-18';

-- Q6: anomalías de costo detectadas por MAD.
SELECT usage_date, service, daily_cost_usd, median_cost_usd, robust_z, anomaly_type
FROM cost_anomalies_by_org
WHERE org_id = 'org_53lc58dr';
