# Muestras de Gold

## cost_anomaly_mart (11050 filas)

```
root
 |-- org_id: string (nullable = true)
 |-- usage_date: date (nullable = true)
 |-- service: string (nullable = true)
 |-- org_name: string (nullable = true)
 |-- daily_cost_usd: double (nullable = true)
 |-- events: long (nullable = true)
 |-- negative_cost_events: long (nullable = true)
 |-- median_cost_usd: double (nullable = true)
 |-- mad_cost_usd: double (nullable = true)
 |-- robust_z: double (nullable = true)
 |-- classic_z: double (nullable = true)
 |-- history_days: long (nullable = true)
 |-- is_anomaly: boolean (nullable = true)
 |-- anomaly_type: string (nullable = true)
```

| org_id | usage_date | service | org_name | daily_cost_usd | events | negative_cost_events | median_cost_usd | mad_cost_usd | robust_z | classic_z | history_days | is_anomaly | anomaly_type |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| org_0lvsnujz | 2025-07-03 | analytics | Nova Cloud 45 | 8.8739 | 3 | 0 | 16.9817 | 8.7194 | -0.6270 | -0.2460 | 50 | False |  |
| org_0lvsnujz | 2025-07-03 | database | Nova Cloud 45 | 0.5723 | 3 | 0 | 8.3471 | 4.6413 | -1.1300 | -1.4010 | 52 | False |  |
| org_0lvsnujz | 2025-07-03 | storage | Nova Cloud 45 | 5.5542 | 3 | 0 | 5.2153 | 1.9873 | 0.1150 | -0.0480 | 54 | False |  |
| org_0lvsnujz | 2025-07-04 | analytics | Nova Cloud 45 | 3.2915 | 3 | 0 | 16.9817 | 8.7194 | -1.0590 | -0.3880 | 50 | False |  |
| org_0lvsnujz | 2025-07-04 | database | Nova Cloud 45 | 12.7337 | 6 | 0 | 8.3471 | 4.6413 | 0.6370 | 0.2120 | 52 | False |  |

## genai_tokens_by_org_date (1131 filas)

```
root
 |-- org_id: string (nullable = true)
 |-- usage_date: date (nullable = true)
 |-- genai_tokens: long (nullable = true)
 |-- genai_cost_usd: double (nullable = true)
 |-- genai_events: long (nullable = true)
 |-- events_with_tokens: long (nullable = true)
 |-- cost_per_1k_tokens_usd: double (nullable = true)
 |-- token_coverage: double (nullable = true)
```

| org_id | usage_date | genai_tokens | genai_cost_usd | genai_events | events_with_tokens | cost_per_1k_tokens_usd | token_coverage |
|---|---|---|---|---|---|---|---|
| org_253j2d54 | 2025-07-09 |  | 26.4375 | 3 | 0 |  | 0.0000 |
| org_253j2d54 | 2025-07-13 |  | 31.9544 | 3 | 0 |  | 0.0000 |
| org_253j2d54 | 2025-07-15 |  | 31.5928 | 3 | 0 |  | 0.0000 |
| org_253j2d54 | 2025-07-16 |  | 23.1240 | 3 | 0 |  | 0.0000 |
| org_253j2d54 | 2025-07-17 |  | 25.7305 | 3 | 0 |  | 0.0000 |

## org_daily_usage_by_service (11050 filas)

```
root
 |-- org_id: string (nullable = true)
 |-- service: string (nullable = true)
 |-- org_name: string (nullable = true)
 |-- daily_cost_usd: double (nullable = true)
 |-- gross_cost_usd: double (nullable = true)
 |-- requests: double (nullable = true)
 |-- cpu_hours: double (nullable = true)
 |-- storage_gb_hours: double (nullable = true)
 |-- genai_tokens: long (nullable = true)
 |-- carbon_kg: double (nullable = true)
 |-- events: long (nullable = true)
 |-- negative_cost_events: long (nullable = true)
 |-- late_events: long (nullable = true)
 |-- carbon_coverage: double (nullable = true)
 |-- usage_date: date (nullable = true)
```

| org_id | service | org_name | daily_cost_usd | gross_cost_usd | requests | cpu_hours | storage_gb_hours | genai_tokens | carbon_kg | events | negative_cost_events | late_events | carbon_coverage | usage_date |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| org_0lvsnujz | analytics | Nova Cloud 45 | 8.8739 | 8.8739 | 116.0000 | 4.6168 | 12.7808 |  |  | 3 | 0 | 3 | 0.0000 | 2025-07-03 |
| org_0lvsnujz | database | Nova Cloud 45 | 0.5723 | 0.5723 |  | 1.7515 | 14.7891 |  |  | 3 | 0 | 3 | 0.0000 | 2025-07-03 |
| org_0lvsnujz | storage | Nova Cloud 45 | 5.5542 | 5.5542 | 255.0000 |  | 4.6668 |  |  | 3 | 0 | 3 | 0.0000 | 2025-07-03 |
| org_0lvsnujz | analytics | Nova Cloud 45 | 3.2915 | 3.2915 |  | 2.7659 | 38.5678 |  |  | 3 | 0 | 3 | 0.0000 | 2025-07-04 |
| org_0lvsnujz | database | Nova Cloud 45 | 12.7337 | 12.7337 | 235.0000 | 2.5139 | 41.2334 |  |  | 6 | 0 | 5 | 0.0000 | 2025-07-04 |

## revenue_by_org_month (240 filas)

```
root
 |-- org_id: string (nullable = true)
 |-- month: date (nullable = true)
 |-- currency: string (nullable = true)
 |-- fx_rate_applied: double (nullable = true)
 |-- invoices: long (nullable = true)
 |-- subtotal_usd: double (nullable = true)
 |-- credits_usd: double (nullable = true)
 |-- taxes_usd: double (nullable = true)
 |-- revenue_usd: double (nullable = true)
 |-- sign_corrected: boolean (nullable = true)
 |-- fx_forced: boolean (nullable = true)
 |-- credits_imputed: boolean (nullable = true)
```

| org_id | month | currency | fx_rate_applied | invoices | subtotal_usd | credits_usd | taxes_usd | revenue_usd | sign_corrected | fx_forced | credits_imputed |
|---|---|---|---|---|---|---|---|---|---|---|---|
| org_0lvsnujz | 2025-06-01 | ARS | 0.0015 | 1 | 2.0000 | 0.0000 | 0.4200 | 2.4200 | False | False | True |
| org_0lvsnujz | 2025-07-01 | USD | 1.0000 | 1 | 1600.2200 | 21.0000 | 336.0500 | 1915.2700 | False | True | False |
| org_0lvsnujz | 2025-08-01 | USD | 1.0000 | 1 | 675.0700 | 0.0000 | 141.7700 | 816.8400 | False | True | True |
| org_0lzjjege | 2025-06-01 | ARS | 0.0014 | 1 | 1.6400 | 0.0000 | 0.3400 | 1.9800 | False | False | True |
| org_0lzjjege | 2025-07-01 | ARS | 0.0015 | 1 | 0.4700 | 0.0100 | 0.1000 | 0.5600 | False | False | False |

## tickets_by_org_date (984 filas)

```
root
 |-- org_id: string (nullable = true)
 |-- ticket_date: date (nullable = true)
 |-- severity: string (nullable = true)
 |-- tickets: long (nullable = true)
 |-- sla_breached: long (nullable = true)
 |-- sla_breach_rate: double (nullable = true)
 |-- csat_avg: double (nullable = true)
 |-- csat_responses: long (nullable = true)
 |-- open_tickets: long (nullable = true)
 |-- avg_resolution_days: double (nullable = true)
```

| org_id | ticket_date | severity | tickets | sla_breached | sla_breach_rate | csat_avg | csat_responses | open_tickets | avg_resolution_days |
|---|---|---|---|---|---|---|---|---|---|
| org_0lvsnujz | 2025-05-25 | critical | 1 | 0 | 0.0000 |  | 0 | 0 | 3.0000 |
| org_0lvsnujz | 2025-06-08 | high | 1 | 0 | 0.0000 |  | 0 | 0 | 12.0000 |
| org_0lvsnujz | 2025-06-09 | high | 1 | 0 | 0.0000 | 1.0000 | 1 | 1 |  |
| org_0lvsnujz | 2025-06-28 | medium | 1 | 0 | 0.0000 | 4.0000 | 1 | 0 | 0.0000 |
| org_0lvsnujz | 2025-07-02 | high | 1 | 0 | 0.0000 | 3.0000 | 1 | 0 | 25.0000 |

## tickets_by_severity_date (364 filas)

```
root
 |-- severity: string (nullable = true)
 |-- ticket_date: date (nullable = true)
 |-- tickets: long (nullable = true)
 |-- sla_breached: long (nullable = true)
 |-- sla_breach_rate: double (nullable = true)
 |-- orgs_affected: long (nullable = true)
 |-- csat_avg: double (nullable = true)
```

| severity | ticket_date | tickets | sla_breached | sla_breach_rate | orgs_affected | csat_avg |
|---|---|---|---|---|---|---|
| low | 2025-05-09 | 1 | 0 | 0.0000 | 1 |  |
| medium | 2025-05-09 | 1 | 0 | 0.0000 | 1 | 3.0000 |
| high | 2025-05-10 | 1 | 0 | 0.0000 | 1 | 6.0000 |
| low | 2025-05-10 | 5 | 1 | 0.2000 | 5 | 3.5000 |
| medium | 2025-05-10 | 4 | 0 | 0.0000 | 3 | 3.0000 |

## top_services_14d (15599 filas)

```
root
 |-- org_id: string (nullable = true)
 |-- as_of_date: date (nullable = true)
 |-- service: string (nullable = true)
 |-- cost_usd_window: double (nullable = true)
 |-- requests_window: double (nullable = true)
 |-- active_days: long (nullable = true)
 |-- cost_rank: integer (nullable = true)
 |-- window_days: integer (nullable = true)
```

| org_id | as_of_date | service | cost_usd_window | requests_window | active_days | cost_rank | window_days |
|---|---|---|---|---|---|---|---|
| org_0lvsnujz | 2025-07-03 | analytics | 8.8739 | 116.0000 | 1 | 1 | 14 |
| org_0lvsnujz | 2025-07-04 | analytics | 12.1654 | 116.0000 | 2 | 2 | 14 |
| org_0lvsnujz | 2025-07-07 | analytics | 37.4039 | 480.0000 | 5 | 1 | 14 |
| org_0lvsnujz | 2025-07-05 | analytics | 19.6696 | 243.0000 | 3 | 2 | 14 |
| org_0lvsnujz | 2025-07-06 | analytics | 27.9501 | 363.0000 | 4 | 1 | 14 |
