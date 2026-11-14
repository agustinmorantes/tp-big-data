-- Tablas de serving, modeladas query-first (ADR-009, ADR-012): una tabla por patrón de acceso.
-- Todas se cargan desde Spark con INSERT (= upsert en Cassandra): re-cargar pisa la misma fila.
-- Se ejecutan dentro del keyspace configurado (USE lo hace src/cpa/serving.py).

-- Q1: costos y requests diarios por organización y servicio en un rango de fechas.
-- Partición = org_id (≈ 60 días × 6 servicios = 354 filas por org); clustering por fecha DESC
-- para leer el rango más reciente primero, y servicio para que la clave sea el grano del mart.
CREATE TABLE IF NOT EXISTS org_daily_usage_by_service (
  org_id               text,
  usage_date           date,
  service              text,
  org_name             text,
  daily_cost_usd       double,
  gross_cost_usd       double,
  requests             double,
  cpu_hours            double,
  storage_gb_hours     double,
  genai_tokens         bigint,
  carbon_kg            double,
  carbon_coverage      double,
  events               bigint,
  negative_cost_events bigint,
  late_events          bigint,
  PRIMARY KEY ((org_id), usage_date, service)
) WITH CLUSTERING ORDER BY (usage_date DESC, service ASC);

-- Q2: top-N servicios por costo acumulado en los últimos 14 días para una organización.
-- Cassandra no ordena por agregados: Gold precalcula el ranking por fecha de corte y la tabla lo
-- guarda ordenado por cost_rank. La consulta es un LIMIT N sobre una sola partición.
CREATE TABLE IF NOT EXISTS top_services_by_org_14d (
  org_id          text,
  as_of_date      date,
  cost_rank       int,
  service         text,
  cost_usd_window double,
  requests_window double,
  active_days     bigint,
  window_days     int,
  PRIMARY KEY ((org_id, as_of_date), cost_rank)
) WITH CLUSTERING ORDER BY (cost_rank ASC);

-- Q3: evolución diaria de tickets críticos y tasa de SLA breach (últimos 30 días).
-- Partición por severidad: la consulta pide una severidad y un rango de días.
CREATE TABLE IF NOT EXISTS tickets_by_severity_date (
  severity        text,
  ticket_date     date,
  tickets         bigint,
  sla_breached    bigint,
  sla_breach_rate double,
  orgs_affected   bigint,
  csat_avg        double,
  PRIMARY KEY ((severity), ticket_date)
) WITH CLUSTERING ORDER BY (ticket_date DESC);

-- Q3 por organización (mismo grano que el mart tickets_by_org_date).
CREATE TABLE IF NOT EXISTS tickets_by_org_date (
  org_id              text,
  ticket_date         date,
  severity            text,
  tickets             bigint,
  sla_breached        bigint,
  sla_breach_rate     double,
  csat_avg            double,
  csat_responses      bigint,
  open_tickets        bigint,
  avg_resolution_days double,
  PRIMARY KEY ((org_id), ticket_date, severity)
) WITH CLUSTERING ORDER BY (ticket_date DESC, severity ASC);

-- Q4: revenue mensual con créditos e impuestos, normalizado a USD.
CREATE TABLE IF NOT EXISTS revenue_by_org_month (
  org_id          text,
  month           date,
  currency        text,
  fx_rate_applied double,
  invoices        bigint,
  subtotal_usd    double,
  credits_usd     double,
  taxes_usd       double,
  revenue_usd     double,
  sign_corrected  boolean,
  fx_forced       boolean,
  credits_imputed boolean,
  PRIMARY KEY ((org_id), month)
) WITH CLUSTERING ORDER BY (month DESC);

-- Q5: tokens GenAI y costo estimado por día (solo existen desde schema v2).
CREATE TABLE IF NOT EXISTS genai_tokens_by_org_date (
  org_id                 text,
  usage_date             date,
  genai_tokens           bigint,
  genai_cost_usd         double,
  genai_events           bigint,
  events_with_tokens     bigint,
  token_coverage         double,
  cost_per_1k_tokens_usd double,
  PRIMARY KEY ((org_id), usage_date)
) WITH CLUSTERING ORDER BY (usage_date DESC);

-- Anomalías de costo (componente analítico): solo los días marcados, por organización.
CREATE TABLE IF NOT EXISTS cost_anomalies_by_org (
  org_id          text,
  usage_date      date,
  service         text,
  org_name        text,
  daily_cost_usd  double,
  median_cost_usd double,
  mad_cost_usd    double,
  robust_z        double,
  classic_z       double,
  anomaly_type    text,
  PRIMARY KEY ((org_id), usage_date, service)
) WITH CLUSTERING ORDER BY (usage_date DESC, service ASC);
