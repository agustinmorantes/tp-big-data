# Decisiones técnicas

Lo que decidimos y por qué, con la alternativa que descartamos. Si una decisión se revierte,
agregamos la nueva y dejamos la anterior con la referencia. Lo que todavía no decidimos está en
`plan-inicial.md`.

**ADR-001 — Parquet de Bronze en adelante.** Landing conserva el CSV/JSONL original sin tocar.
Parquet es columnar y comprimido, y soporta merge de esquema, que es lo que hace falta para el
cambio v1 a v2 del 18/07. Descartamos Delta Lake: resolvería la idempotencia más limpio, pero
agrega una dependencia que el caso todavía no necesita.

**ADR-002 — Patrón híbrido.** Batch para las 7 fuentes maestras y billing, Structured Streaming
para `usage_events_stream`. De las 8 fuentes, solo esa tiene naturaleza de evento: 43 200 eventos
contra 3 712 filas batch. El objetivo de 3 minutos descarta batch puro; Kappa obligaría a tratar
los maestros como un log de eventos y Lambda duplicaría la lógica en dos pipelines.

**ADR-003 — Partición por fecha, no por `org_id`.** Con 80 organizaciones, particionar por
`org_id` genera muchos archivos chicos, que sale más caro que escanear de más. La fecha es además
el filtro de casi todas las consultas. En Gold se agrega `org_id` como segundo nivel solo donde
el mart lo pide.

**ADR-004 — Bronze no descarta registros.** Tipa, agrega las columnas técnicas y escribe en
append. La calidad y el quarantine van en el paso a Silver, para poder reprocesar desde Bronze
sin volver a Landing cuando cambian las reglas. Como consecuencia Bronze contiene registros
inválidos, y toda lectura aguas abajo tiene que asumirlo.

**ADR-005 — `nps_surveys` es la fuente de verdad del NPS.** 44 organizaciones tienen valor en
`customers_orgs.nps_score` y en `nps_surveys`, y en ninguna coinciden: no son reconciliables.
`nps_surveys` está limpio y tiene fecha; el maestro contiene el único valor inválido del dataset
(101). Cuesta cobertura —60 de 80 organizaciones— y el mart lo declara.

**ADR-006 — Los subtotales negativos de billing se corrigen.** 13 de 240 facturas tienen
`subtotal` negativo, pero `taxes / |subtotal|` da 0.21 igual que en las 227 positivas: el impuesto
está calculado sobre el valor absoluto correcto, así que lo único corrompido es el signo. Se
multiplican por −1 con flag `_corregido_signo`. Una fila negativa sin ese ratio va a quarantine.

**ADR-007 — El tipo de cambio de las facturas USD se fuerza a 1.** Ninguna de las 160 facturas
USD tiene tasa 1, que es su valor por definición, aunque están centradas en 1 y el efecto sobre
el total es de 0,12 %. Se corrige porque por factura el desvío llega al 14,5 % y el mart reporta
por organización. ARS y EUR tienen dispersión equivalente, pero sin valor de referencia conocido
no hay contra qué contrastarlas: se usan como vienen.

**ADR-008 — La deduplicación por `event_id` es defensiva.** Los 43 200 eventos tienen 43 200
`event_id` distintos. El `dropDuplicates` acotado por watermark no corrige el origen: protege
contra reprocesos, reenvíos y relecturas tras un fallo de checkpoint. Es idempotencia, no calidad.

**ADR-009 — Cassandra/AstraDB como serving.** Tablas modeladas query-first, una por patrón de
acceso. Las consultas se conocen de antemano y filtran por `org_id` y rango de fechas. El costo
es que no hay agregaciones ad-hoc: todo viene precalculado de Gold y una pregunta nueva implica
una tabla nueva.
