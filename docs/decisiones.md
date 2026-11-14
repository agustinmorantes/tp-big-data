# Decisiones técnicas

Acá registramos lo que decidimos, por qué y qué alternativa descartamos. Si revertimos una
decisión, agregamos la nueva y dejamos la anterior con la referencia. Lo que todavía está abierto
figura en `backlog.md`.

## Primera entrega (diseño)

**ADR-001. Parquet desde Bronze en adelante.** Landing guarda el CSV o JSONL original sin
modificar. Parquet es columnar, está comprimido y permite unir esquemas distintos, que es lo que
necesitamos para el cambio de v1 a v2 del 18/07. Descartamos Delta Lake: resolvería mejor la
idempotencia, pero suma una dependencia que por ahora el caso no necesita.

**ADR-002. Patrón híbrido.** Batch para las 7 fuentes maestras y la facturación, y Structured
Streaming para `usage_events_stream`. De las 8 fuentes, es la única que es un flujo de eventos
(43.200 eventos contra 4.112 filas batch). El objetivo de 3 minutos descarta hacer todo en batch.
Kappa nos obligaría a tratar los maestros como un log de eventos, y Lambda duplicaría la lógica
en dos pipelines.

**ADR-003. Particionar por fecha y no por `org_id`.** Con 80 organizaciones, particionar por
`org_id` genera muchos archivos chicos, y eso sale más caro que leer de más. Además, la fecha es
el filtro de casi todas las consultas. En Gold agregamos `org_id` como segundo nivel solo donde
el mart lo necesita.

**ADR-004. Bronze no descarta registros.** En Bronze solo tipamos, agregamos las columnas
técnicas y escribimos en modo append. La calidad y el quarantine se hacen al pasar a Silver, así
si cambian las reglas podemos reprocesar desde Bronze sin volver a Landing. La contra es que
Bronze tiene registros inválidos y todo lo que lee de Bronze tiene que contemplarlo.

**ADR-005. El NPS sale de `nps_surveys`.** 44 organizaciones tienen valor en
`customers_orgs.nps_score` y en `nps_surveys`, y en ninguna coinciden, así que no se pueden
reconciliar. `nps_surveys` está limpio y tiene fecha, mientras que el maestro tiene el único
valor inválido del dataset (101). Perdemos cobertura (60 de 80 organizaciones) y el mart lo
aclara.

**ADR-006. Corregimos los subtotales negativos de facturación.** 13 de las 240 facturas tienen
`subtotal` negativo, pero `taxes / |subtotal|` da 0,21 igual que en las 227 positivas. O sea, el
impuesto se calculó sobre el valor absoluto y lo único que está mal es el signo. Las
multiplicamos por −1 y les ponemos un flag. Si una factura negativa no cumple ese ratio, va a
quarantine.

**ADR-007. Forzamos a 1 el tipo de cambio de las facturas en USD.** Ninguna de las 160 facturas
en USD tiene tasa 1, que es lo que debería tener. En total el efecto es de 0,12 % porque los
valores están centrados en 1, pero en una factura puntual el desvío llega a 14,5 %, y el mart
reporta por organización. ARS y EUR tienen una dispersión parecida, pero como no sabemos cuál
es el valor correcto, las usamos como vienen.

**ADR-008. Deduplicar por `event_id` es preventivo.** Los 43.200 eventos tienen 43.200
`event_id` distintos. El dedupe no corrige un problema del origen: nos protege de reprocesos,
reenvíos y relecturas si se pierde el checkpoint. Es parte de la idempotencia, no de la calidad.

**ADR-009. Cassandra/AstraDB para el serving.** Una tabla por patrón de acceso, diseñada a
partir de la consulta. Las consultas se conocen de antemano y filtran por `org_id` y rango de
fechas. La contra es que no se pueden hacer agregaciones ad hoc: todo viene calculado desde Gold,
y una pregunta nueva implica una tabla nueva.

## Segunda entrega (implementación)

**ADR-010. Particiones del lake.** En Bronze batch, los maestros se particionan por
`ingest_date` (fecha de carga, porque no tienen una fecha de negocio útil), `billing_monthly` por
`billing_month`, `support_tickets` por `created_month` y `marketing_touches` por `touch_month`.
Bronze de eventos se particiona por `ingest_date`, es decir, por fecha de llegada. Si lo
particionáramos por fecha de evento, cada micro-batch abriría 60 particiones, porque cada JSONL
trae eventos de todo el período. Silver de eventos sí se particiona por `event_date`, que es el
filtro que usa Gold. El replay deja un archivo por micro-batch y fecha, y la etapa `compact` los
junta en uno por fecha (R-5). En Gold, `org_daily_usage_by_service` se particiona por
`usage_date`. Los demás marts son chicos (menos de 10.000 filas) y van en un solo archivo. Antes
de escribir hacemos `repartition` por la columna de partición, así queda un archivo por
partición.

**ADR-011. Eventos tardíos: umbral propio en `foreachBatch` y watermark nativo solo en la vista
de velocidad.** Los 120 JSONL no están ordenados por fecha de evento: cada archivo tiene eventos
del 03/07 al 31/08. Con un `withWatermark` nativo, casi todo lo que llega después del primer
batch se descarta (la etapa `speed-view` lo mide, ver `evidence/segunda-entrega/streaming.md`).
Por eso separamos dos caminos:

- En el camino principal (Bronze a Silver) no descartamos ningún evento por tardío. Lo marcamos
  con `is_late = event_ts < max(event_ts de los batches anteriores) − 6 h` y entra igual a
  Silver. Como Gold se recalcula entero, un evento tardío corrige el agregado de su día en la
  corrida siguiente. Guardamos el máximo por `batch_id` en
  `_checkpoints/silver_usage_events/late_data_state.json`, así si un batch se reintenta usa el
  mismo corte.
- En la vista de velocidad (`speed-view`) agregamos por ventanas de 1 hora con
  `withWatermark("event_ts", "10 minutes")` y escribimos en memoria. Sirve para mostrar el
  mecanismo nativo y medir cuánto se perdería (`numRowsDroppedByWatermark`).

Elegimos 6 horas porque en una operación real los eventos llegan con minutos de demora, y 6 horas
cubren la caída de un colector sin marcar como tardío el tráfico normal. Se puede cambiar en
`streaming.late_threshold_hours`.

**ADR-012. Dedupe por `event_id` contra Silver en lugar de `dropDuplicates` con watermark
(revisa ADR-008).** Por lo mismo que en ADR-011, un `dropDuplicates` limitado por watermark no
funciona con este replay. En cada micro-batch primero deduplicamos dentro del batch
(`row_number` por `event_id`, se queda el primer archivo) y después hacemos un anti-join contra
los `event_id` que ya están en Silver. Los duplicados van a quarantine con el motivo
(`duplicate_event_id_in_batch` o `duplicate_event_id_already_in_silver`). Así es idempotente
aunque se pierda el checkpoint; lo probamos en la etapa `idempotency` y en
`tests/test_stream_e2e.py`. La contra es que el anti-join lee la columna `event_id` de Silver en
cada batch. Con 43.000 eventos no importa, pero a escala habría que limitarlo a un rango de
`event_date`.

**ADR-013. Serving con el driver de Python desde `foreachPartition`, sin
spark-cassandra-connector.** El conector necesita un JAR que depende de la versión de Spark y
Scala, y para AstraDB hay que tener el bundle en cada executor. El driver `cassandra-driver`
funciona igual en Docker, en local y en Colab, contra Cassandra o contra Astra. Cada partición
abre una sesión y ejecuta `INSERT` preparados. En Cassandra un `INSERT` sobre una clave que ya
existe la reemplaza, así que volver a cargar no duplica. Hay una tabla por consulta
(`cql/01_tables.cql`). El top N lo calculamos en Gold porque Cassandra no ordena por agregados.
La clave de partición es `org_id` (unas 354 filas por organización en el mart diario, muy por
debajo del límite recomendado) y las filas se ordenan por fecha descendente para leer primero lo
más reciente.

**ADR-014. Anomalías de costo con z-score robusto (MAD) y umbral 3,5.** Para cada organización y
servicio tomamos la serie de costo diario y calculamos `z = 0,6745 · (x − mediana) / MAD`. Es
anomalía si `|z| > 3,5` (criterio de Iglewicz y Hoaglin), y pedimos al menos 14 días de historia.
Usamos mediana y MAD porque el costo por evento tiene una cola muy pesada (el máximo es unas 19
veces el p99): la media y el desvío se inflan con los mismos picos que queremos detectar. También
calculamos el z clásico para comparar (`evidence/segunda-entrega/anomalies.md`). La limitación es
que la referencia es toda la historia; el paso siguiente sería una ventana móvil, para que un
cambio de nivel que se sostiene deje de contar como anomalía.

**ADR-015. Una sola estructura de quarantine con el registro original.** Todas las fuentes
escriben en `quarantine/<tabla>` con el mismo esquema: `reasons`, `primary_reason`,
`disposition` y `record_json` con la fila tal como llegó. `rejected` quiere decir que la fila no
entra a Silver; `remediated`, que entra corregida y con flag, y acá queda la original. Guardar el
registro como JSON nos deja conservar el valor original (por ejemplo, un `value` que no es
número) sin pelear con esquemas distintos. Los flags de facturación terminaron siendo columnas
booleanas (`sign_corrected`, `fx_forced`, `credits_imputed`), no el `_corregido_signo` que
planteaba ADR-006.

**ADR-016. Docker como entorno de referencia y Colab como alternativa.** Spark en Windows
necesita `winutils` y `HADOOP_HOME`. Para no depender de eso, el entorno de referencia es
`infra/docker-compose.yml`, con Python 3.11, OpenJDK 17, PySpark 3.5.1 y Cassandra 4.1. El
notebook `notebooks/02_pipeline_segunda_entrega.ipynb` corre el mismo código en Colab contra
AstraDB. Lo único que cambia entre los dos es `CPA_SERVING_TARGET`.
