# Plan de correcciones

Correcciones a la primera entrega, a partir de la devolución y de una revisión propia del
informe. Las del informe las aplicamos en la versión de la segunda entrega, y las del
repositorio también están hechas.

Cada tabla está ordenada por prioridad.

## Informe

| # | Corrección | Prioridad | Responsable | Fecha | Evidencia | Estado |
|---|---|---|---|---|---|---|
| C-1 | Juntar en una sección final del informe quién hace qué, el esfuerzo, los riesgos principales y las decisiones abiertas | Alta | Morantes | 16/11/2026 | Sección final del informe, armada con `plan-inicial.md` y `backlog.md` | Hecho |
| C-2 | Ordenar por prioridad los riesgos, las decisiones abiertas y las correcciones | Alta | Morantes | 16/11/2026 | Este plan y `backlog.md`, y lo mismo en el informe | Hecho |
| C-3 | Explicar por qué el caso justifica Big Data aunque la muestra entre en un solo servidor | Alta | Equipo | 16/11/2026 | Cierre de la sección 2 del informe | Hecho |
| C-4 | Poner una meta numérica a cada objetivo, no solo a la latencia | Media | Equipo | 16/11/2026 | Sección 1.3 del informe | Hecho |
| C-5 | Nombrar la arquitectura Medallion y explicar por qué agregamos Landing | Media | Morantes | 16/11/2026 | Sección 7.1 del informe | Hecho |
| C-6 | Agregar el combiner a la lógica MapReduce (`reduceByKey` agrega en cada partición antes del shuffle) | Media | Lombardo | 16/11/2026 | Fila nueva en la tabla de la sección 9 | Hecho |
| C-7 | Agregar notas del perfilado de las fuentes | Media | Lombardo | 16/11/2026 | Sección 3.3 del informe | Hecho |
| C-8 | Reemplazar el diagrama v1 por el v2 | Baja | Morantes | 16/11/2026 | Sección 4 del informe | Hecho en el repo, falta reemplazar la imagen en el informe |

## Repositorio

| # | Corrección | Prioridad | Responsable | Fecha | Evidencia | Estado |
|---|---|---|---|---|---|---|
| C-9 | Diagrama que refleje lo implementado, con versión, fecha y la capa analítica | Media | Morantes | 16/11/2026 | `docs/arquitectura/arquitectura-v2.svg` | Hecho |
| C-10 | Agrupar las capacidades transversales en Gobierno (catálogo, linaje, propiedad), Calidad, Seguridad y Observabilidad | Baja | Morantes | 16/11/2026 | Banda inferior del diagrama v2 | Hecho |

## Decisiones abiertas del plan inicial

Para poder implementar, las tomamos como supuestos. Cambiar cualquiera es tocar una regla o un
valor de configuración, y los tests lo cubren.

| # | Decisión | Cómo la resolvimos | Dónde |
|---|---|---|---|
| D-1 | `credits` nulo | Lo tomamos como 0, con el flag `credits_imputed` y una copia en quarantine como `remediated` | `quality.apply_billing_rules` |
| D-2 | Escala de CSAT | La dejamos como viene (0 a 7) y promediamos sin reescalar; los nulos no cuentan | `gold.tickets_by_org_date` |
| D-3 | Costos negativos | Son ajustes: entran a Silver con `is_negative_cost`. `daily_cost_usd` es el neto y `gross_cost_usd` no incluye los ajustes | ADR-011, `gold.org_daily_usage_by_service` |
| D-4 | Grano de los marts | Los que pide el enunciado, más `top_services_14d` y `tickets_by_severity_date` para las consultas 2 y 3 | `docs/diccionario-datos.md` |
| D-5 | Watermark | Umbral de 6 h para eventos tardíos en Silver; el watermark nativo de 10 min solo en la vista de velocidad | ADR-011 |

D-1, D-2 y D-3 todavía tenemos que confirmarlas con la cátedra (ver `backlog.md`).
