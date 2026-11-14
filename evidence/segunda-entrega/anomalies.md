# Componente analítico: anomalías de costo (MAD)

- Días (org, servicio) puntuados: **11050**
- Anomalías por z robusto (|z| > 3.5): **148** (1.34 %)
- Anomalías por z clásico (|z| > 3): **98**; coinciden con el robusto: **90**
- Días con al menos un evento > 10× el p99 de costo por evento (p99 = 16.70 USD): **34**; detectados por MAD: **34** (100.0 %)

## Por tipo

| anomaly_type | count |
|---|---|
| spike | 135 |
| drop | 13 |

## Top 15 por z robusto

| org_id | usage_date | service | daily_cost_usd | median_cost_usd | mad_cost_usd | robust_z | classic_z |
|---|---|---|---|---|---|---|---|
| org_okep7y6w | 2025-07-03 | compute | 202.6460 | 11.1426 | 2.7475 | 47.0130 | 4.3660 |
| org_x7eedtjv | 2025-07-09 | compute | 311.2550 | 12.9288 | 4.4822 | 44.8930 | 5.1390 |
| org_okep7y6w | 2025-08-07 | compute | 191.8968 | 11.1426 | 2.7475 | 44.3740 | 4.1070 |
| org_d14ve92m | 2025-07-20 | analytics | 181.7988 | 10.5941 | 3.2168 | 35.8980 | 5.6280 |
| org_w3zp08j3 | 2025-07-05 | networking | 27.4203 | 1.4849 | 0.4944 | 35.3830 | 4.2140 |
| org_w3zp08j3 | 2025-07-16 | networking | 26.4157 | 1.4849 | 0.4944 | 34.0130 | 4.0400 |
| org_pnsm43d8 | 2025-07-17 | storage | 56.6704 | 3.2130 | 1.1540 | 31.2450 | 5.7730 |
| org_pht0hl9x | 2025-08-25 | analytics | 257.0420 | 14.6066 | 5.4627 | 29.9340 | 6.0470 |
| org_x7eedtjv | 2025-08-23 | compute | 211.2481 | 12.9288 | 4.4822 | 29.8440 | 3.3330 |
| org_tvhhpbmy | 2025-07-10 | compute | 204.9628 | 10.4343 | 4.4560 | 29.4460 | 5.0520 |
| org_874frhoc | 2025-07-17 | networking | 27.6510 | 1.7080 | 0.6065 | 28.8520 | 5.3460 |
| org_ujv6oh9s | 2025-07-19 | database | 142.7731 | 8.3903 | 3.1660 | 28.6300 | 6.2820 |
| org_g8sbi4q2 | 2025-08-05 | database | 142.9470 | 10.3302 | 3.3226 | 26.9220 | 5.6630 |
| org_ir4cowgz | 2025-07-21 | analytics | 229.7084 | 14.3892 | 5.4922 | 26.4430 | 6.4980 |
| org_he7ur23g | 2025-07-23 | database | 150.5344 | 7.1729 | 3.7943 | 25.4850 | 5.9630 |
