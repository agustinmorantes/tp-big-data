# Backlog para la entrega final (07/12/2026)

Estado al cierre de la segunda entrega. Primero va lo obligatorio; lo deseable solo entra si no
pone en riesgo lo anterior.

## Lo que ya está hecho

- Las 7 fuentes batch en Bronze y Silver, y el streaming de eventos con checkpoint, dedupe y
  manejo de eventos tardíos.
- Los 7 marts de Gold: los 5 que pide el enunciado, más `top_services_14d` y
  `tickets_by_severity_date`, que necesitamos para las consultas 2 y 3.
- 7 tablas en Cassandra cargadas desde Spark. Las 5 consultas de negocio y la de anomalías
  corren contra Cassandra local y contra AstraDB, y su salida está en
  `evidence/segunda-entrega/queries/`.
- Detección de anomalías con MAD, idempotencia probada, 13 tests y el pipeline corriendo en
  Docker y en Colab.

## Obligatorio

| # | Tarea | Responsable | Estimación | Cuándo está hecha |
|---|---|---|---|---|
| O-1 | Correr el pipeline contra AstraDB y guardar la evidencia | Morantes | - | Hecho el 13/11 |
| O-2 | Pasar al informe final las correcciones pendientes | Equipo | 4-6 h | Nada pendiente en `plan-correcciones.md` |
| O-3 | Incorporar la devolución de la segunda entrega | Equipo | 4-8 h | Plan de correcciones actualizado |
| O-4 | Probar el README desde cero en otra máquina y en Colab | Garcia | 2 h | Alguien que no lo escribió lo corre sin ayuda |
| O-5 | Diagrama final consistente con el código | Morantes | 1-2 h | `docs/arquitectura/` actualizado |
| O-6 | Documentación técnica final: modelo de serving y limitaciones | Lombardo | 3-4 h | `docs/` completo |
| O-7 | Presentación ejecutiva | Equipo | 6-8 h | Slides terminadas |
| O-8 | Video mostrando el pipeline y los resultados | Equipo | 4-6 h | Video subido |
| O-9 | Ensayar la demo, incluido el plan B con Cassandra local y la evidencia guardada | Equipo | 2 h | Ensayo cronometrado |

## Riesgos principales

| Prioridad | Riesgo | Estado | Qué hacemos |
|:---:|---|---|---|
| 1 | Que AstraDB no esté disponible el día de la demo (R-8) | Mitigado | Ya guardamos la evidencia de Astra. El plan B es Cassandra local; lo ensayamos en O-9 |
| 2 | Que el README tenga pasos que solo conoce quien lo escribió | Abierto | Lo cubre O-4 |
| 3 | Que la cátedra no acepte los supuestos D-1 a D-3 | Abierto | Cada uno es una regla con tests, así que cambiarlo es acotado |
| 4 | Que se lea como un error que casi todos los eventos queden marcados como tardíos | Mitigado | Está explicado en ADR-011 y en `streaming.md`; lo mostramos en la defensa |

Los demás riesgos del plan inicial (R-1 a R-7, R-9 y R-10) ya están resueltos en el código. Se
puede ver en `quarantine_samples.md`, `idempotencia.md` y `anomalies.md`.

## Decisiones abiertas

En orden de prioridad:

| Prioridad | Decisión | Qué afecta | Responsable | Fecha |
|:---:|---|---|---|---|
| 1 | Confirmar que `credits` nulo es 0 (D-1) | Revenue neto (consulta 4) | Garcia | 20/11/2026 |
| 2 | Confirmar que los costos negativos son ajustes (D-3) | `daily_cost_usd` (consultas 1 y 2, anomalías) | Garcia | 20/11/2026 |
| 3 | Confirmar que CSAT va de 0 a 7 y se promedia sin reescalar (D-2) | `csat_avg` | Lombardo | 20/11/2026 |
| 4 | Usar una referencia fija o una ventana móvil para el MAD | Solo la calidad de la detección | Lombardo | 27/11/2026 |

Cómo resolvimos por ahora D-1 a D-5 está en `plan-correcciones.md`.

## Deseable

| # | Tarea | Por qué | Estimación |
|---|---|---|---|
| D-1 | Ventana móvil de 14 a 28 días para el MAD | Que un cambio de nivel sostenido deje de contar como anomalía (ADR-014) | 3-4 h |
| D-2 | Gold incremental: recalcular solo las fechas que tocó el último batch | Hoy Gold se recalcula entero, y a escala no alcanzaría | 4-6 h |
| D-3 | Limitar el anti-join del dedupe a un rango de `event_date` | Hoy su costo crece con el tamaño de Silver (ADR-012) | 2 h |
| D-4 | Mart de salud de cuenta que cruce NPS con uso y tickets | Le sirve a Customer Success | 4 h |
| D-5 | Tests de los marts de tickets y revenue | Más cobertura de Gold | 2 h |
| D-6 | Notebook con gráficos sobre Cassandra | Mostrar el consumo en la demo | 3-4 h |
| D-7 | Historial de cambios de plan en `customers_orgs` (SCD tipo 2) | Hoy nos quedamos con la última versión | 4-6 h |

## Fuera de alcance

- Delta Lake o Iceberg (ADR-001). Resolverían MERGE y time travel, pero con este volumen no hacen
  falta.
- Un orquestador como Airflow o Dagster. Para el MVP alcanza con correr las etapas por línea de
  comandos.
- Kafka u otra fuente de streaming real. Seguimos con el replay de archivos del dataset.
- Control de acceso real por zona. Lo dejamos documentado como objetivo en `docs/gobierno.md`.
- Modelos de ML supervisados. No hay anomalías etiquetadas, así que usamos MAD.
