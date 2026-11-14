# Quarantine: conteos y muestras

`rejected` = no entra a Silver. `remediated` = entra corregida con flag; acá queda el original.

## Conteo por tabla, disposición y motivo

| source_table | disposition | primary_reason | count |
|---|---|---|---|
| billing_monthly | remediated | usd_fx_forced_to_1 | 150 |
| billing_monthly | remediated | credits_null_as_zero | 45 |
| billing_monthly | remediated | subtotal_sign_corrected | 13 |
| usage_events | rejected | duplicate_event_id_already_in_silver | 43200 |
| usage_events | remediated | unit_imputed_from_metric | 2038 |

### billing_monthly · remediated · `usd_fx_forced_to_1` (150 filas)

| reasons | record_json |
|---|---|
| ['usd_fx_forced_to_1', 'credits_null_as_zero'] | {"org_id":"org_om5igqpk","invoice_id":"inv_081i1ccm","month":"2025-08-01","subtotal":1033.34,"credits":0.0,"taxes":217.0,"currency":"USD","exchange_rate_to_usd":0.9616,"source_file":"file:///app/data/datalake/landing/billing_monthly.csv","ingest_ts":"2026-11-13T19:34:59.000Z","load_id":"20261113T192404Z-eb3465","billing_month":"2025-08","_org_known":true,"sign_corrected":false,"fx_forced":true,"c… |
| ['usd_fx_forced_to_1'] | {"org_id":"org_7e8g8jdp","invoice_id":"inv_0xwxvtx6","month":"2025-08-01","subtotal":306.2,"credits":32.48,"taxes":64.3,"currency":"USD","exchange_rate_to_usd":1.02576,"source_file":"file:///app/data/datalake/landing/billing_monthly.csv","ingest_ts":"2026-11-13T19:34:59.000Z","load_id":"20261113T192404Z-eb3465","billing_month":"2025-08","_org_known":true,"sign_corrected":false,"fx_forced":true,"c… |
| ['usd_fx_forced_to_1', 'credits_null_as_zero'] | {"org_id":"org_dzr1yxr2","invoice_id":"inv_2798p7pr","month":"2025-08-01","subtotal":1217.78,"credits":0.0,"taxes":255.73,"currency":"USD","exchange_rate_to_usd":1.01774,"source_file":"file:///app/data/datalake/landing/billing_monthly.csv","ingest_ts":"2026-11-13T19:34:59.000Z","load_id":"20261113T192404Z-eb3465","billing_month":"2025-08","_org_known":true,"sign_corrected":false,"fx_forced":true,… |

### billing_monthly · remediated · `credits_null_as_zero` (45 filas)

| reasons | record_json |
|---|---|
| ['credits_null_as_zero'] | {"org_id":"org_ohp6vz41","invoice_id":"inv_08cmqxgy","month":"2025-08-01","subtotal":681.35,"credits":0.0,"taxes":143.08,"currency":"EUR","exchange_rate_to_usd":1.19808,"source_file":"file:///app/data/datalake/landing/billing_monthly.csv","ingest_ts":"2026-11-13T19:34:59.000Z","load_id":"20261113T192404Z-eb3465","billing_month":"2025-08","_org_known":true,"sign_corrected":false,"fx_forced":false,… |
| ['credits_null_as_zero'] | {"org_id":"org_id25owf7","invoice_id":"inv_2fm72c6z","month":"2025-08-01","subtotal":1579.19,"credits":0.0,"taxes":331.63,"currency":"ARS","exchange_rate_to_usd":0.00148,"source_file":"file:///app/data/datalake/landing/billing_monthly.csv","ingest_ts":"2026-11-13T19:34:59.000Z","load_id":"20261113T192404Z-eb3465","billing_month":"2025-08","_org_known":true,"sign_corrected":false,"fx_forced":false… |
| ['credits_null_as_zero'] | {"org_id":"org_chj755nf","invoice_id":"inv_2ujz4n1x","month":"2025-08-01","subtotal":1585.03,"credits":0.0,"taxes":332.86,"currency":"ARS","exchange_rate_to_usd":0.00148,"source_file":"file:///app/data/datalake/landing/billing_monthly.csv","ingest_ts":"2026-11-13T19:34:59.000Z","load_id":"20261113T192404Z-eb3465","billing_month":"2025-08","_org_known":true,"sign_corrected":false,"fx_forced":false… |

### billing_monthly · remediated · `subtotal_sign_corrected` (13 filas)

| reasons | record_json |
|---|---|
| ['subtotal_sign_corrected', 'credits_null_as_zero'] | {"org_id":"org_ktakpuxq","invoice_id":"inv_0kg6umh4","month":"2025-08-01","subtotal":1355.74,"credits":0.0,"taxes":284.71,"currency":"ARS","exchange_rate_to_usd":0.00157,"source_file":"file:///app/data/datalake/landing/billing_monthly.csv","ingest_ts":"2026-11-13T19:34:59.000Z","load_id":"20261113T192404Z-eb3465","billing_month":"2025-08","_org_known":true,"sign_corrected":true,"fx_forced":false,… |
| ['subtotal_sign_corrected', 'usd_fx_forced_to_1'] | {"org_id":"org_x7eedtjv","invoice_id":"inv_3tlo1i7x","month":"2025-08-01","subtotal":879.97,"credits":56.93,"taxes":184.79,"currency":"USD","exchange_rate_to_usd":0.95074,"source_file":"file:///app/data/datalake/landing/billing_monthly.csv","ingest_ts":"2026-11-13T19:34:59.000Z","load_id":"20261113T192404Z-eb3465","billing_month":"2025-08","_org_known":true,"sign_corrected":true,"fx_forced":true,… |
| ['subtotal_sign_corrected', 'usd_fx_forced_to_1'] | {"org_id":"org_pac56t4u","invoice_id":"inv_4dqxin67","month":"2025-08-01","subtotal":701.47,"credits":2.71,"taxes":147.31,"currency":"USD","exchange_rate_to_usd":0.96736,"source_file":"file:///app/data/datalake/landing/billing_monthly.csv","ingest_ts":"2026-11-13T19:34:59.000Z","load_id":"20261113T192404Z-eb3465","billing_month":"2025-08","_org_known":true,"sign_corrected":true,"fx_forced":true,"… |

### usage_events · rejected · `duplicate_event_id_already_in_silver` (43200 filas)

| reasons | record_json |
|---|---|
| ['duplicate_event_id_already_in_silver'] | {"org_id":"org_gv0e38da","event_id":"evt_02mwfekre3nt","timestamp":"2025-08-09T15:27:00Z","resource_id":"res_llgf309h","service":"networking","region":"ap-northeast","metric":"storage_gb_hours","value":"0.0","unit":"gb_hours","cost_usd_increment":0.0,"schema_version":2,"carbon_kg":0.0,"event_ts":"2025-08-09T15:27:00.000Z","ingest_ts":"2026-11-13T19:25:17.842Z","source_file":"file:///app/data/data… |
| ['duplicate_event_id_already_in_silver'] | {"org_id":"org_pht0hl9x","event_id":"evt_03xjr7e7ikax","timestamp":"2025-08-16T16:45:00Z","resource_id":"res_ulyo3pjb","service":"compute","region":"us-west","metric":"requests","value":"135.0","unit":"count","cost_usd_increment":9.1453,"schema_version":2,"carbon_kg":0.027,"event_ts":"2025-08-16T16:45:00.000Z","ingest_ts":"2026-11-13T19:25:17.842Z","source_file":"file:///app/data/datalake/landing… |
| ['duplicate_event_id_already_in_silver'] | {"org_id":"org_53lc58dr","event_id":"evt_041ie8hgbsuf","timestamp":"2025-08-06T18:12:00Z","resource_id":"res_nbhgpbkt","service":"networking","region":"ap-northeast","metric":"storage_gb_hours","value":"7.7178","unit":"gb_hours","cost_usd_increment":0.0642,"schema_version":2,"carbon_kg":0.001544,"event_ts":"2025-08-06T18:12:00.000Z","ingest_ts":"2026-11-13T19:25:17.842Z","source_file":"file:///ap… |

### usage_events · remediated · `unit_imputed_from_metric` (2038 filas)

| reasons | record_json |
|---|---|
| ['unit_imputed_from_metric'] | {"event_id":"evt_3dx8n4o5crhr","org_id":"org_1swjckjl","timestamp":"2025-08-08T12:38:00Z","resource_id":"res_3lww7opi","service":"compute","region":"ap-south","metric":"storage_gb_hours","value":"12.0397","unit":"gb_hours","cost_usd_increment":1.0692,"schema_version":2,"carbon_kg":0.002408,"event_ts":"2025-08-08T12:38:00.000Z","ingest_ts":"2026-11-13T19:25:00.584Z","source_file":"file:///app/data… |
| ['unit_imputed_from_metric'] | {"event_id":"evt_3j70nynsyckk","org_id":"org_i3qk2iag","timestamp":"2025-07-30T05:56:00Z","resource_id":"res_oulwozq4","service":"compute","region":"eu-central","metric":"requests","value":"119.0","unit":"count","cost_usd_increment":8.8903,"schema_version":2,"carbon_kg":0.0238,"event_ts":"2025-07-30T05:56:00.000Z","ingest_ts":"2026-11-13T19:25:00.584Z","source_file":"file:///app/data/datalake/lan… |
| ['unit_imputed_from_metric'] | {"event_id":"evt_5ss4e2pwas3f","org_id":"org_0lvsnujz","timestamp":"2025-08-05T06:46:00Z","resource_id":"res_bwsxxdyp","service":"analytics","region":"us-east","metric":"storage_gb_hours","value":"14.8025","unit":"gb_hours","cost_usd_increment":0.9734,"schema_version":2,"carbon_kg":0.002961,"event_ts":"2025-08-05T06:46:00.000Z","ingest_ts":"2026-11-13T19:25:00.584Z","source_file":"file:///app/dat… |
