# Plan inicial

Supuestos, riesgos, decisiones abiertas, esfuerzo, roles y próximos pasos. Todo preliminar: se
revisa en cada entrega.

## Supuestos

| # | Supuesto | Cómo lo validamos | Si es falso |
|---|---|---|---|
| S-1 | `credits` nulo es $0, no dato faltante (137 de 240 facturas) | Preguntar al docente | El revenue neto queda sobreestimado |
| S-2 | `resolved_at` nulo es ticket abierto (240 de 1000) | Los nulos deberían concentrarse en los tickets recientes | Las métricas de SLA quedan mal |
| S-3 | Lo perturbado es `exchange_rate_to_usd`, no `currency` | Confirmar con quien genera el archivo | La corrección de ADR-007 rompe los montos en vez de arreglarlos |
| S-4 | Los `cost_usd_increment` negativos son ajustes válidos (216 de 43 200) | Ver si se compensan con un evento positivo del mismo recurso | El costo diario queda subestimado |
| S-5 | `event_id` es único y estable, sirve como clave de idempotencia | Verificado: 43 200 de 43 200 | Los upserts a Cassandra duplican filas |
| S-6 | La muestra no representa el volumen productivo | — | Cambia todo el criterio de particionado |

## Riesgos y mitigaciones

| # | Riesgo | P | I | Mitigación |
|---|---|---|---|---|
| R-1 | Moneda mixta sin conversión: sumar USD, ARS y EUR da revenue sin sentido | Alta | Alto | Normalizar a USD en Silver forzando tasa 1 para USD (ADR-007), con flag |
| R-2 | Evolución de esquema v1 a v2: un esquema rígido rompe con los eventos anteriores al 18/07 | Alta | Alto | Declarar `carbon_kg` y `genai_tokens` nullable desde el arranque; los marts agregan solo sobre el subconjunto que tiene el dato |
| R-3 | Confundir nulo esperado con dato faltante: `resolved_at` y `csat` nulos son estados válidos | Alta | Alto | Reglas de calidad por columna, nunca un `dropna()` global |
| R-4 | Duplicados por reproceso del streaming | Alta | Alto | Checkpoint persistente y upsert con `event_id` en la clave primaria (ADR-008) |
| R-5 | Micro-batches cada 3 minutos generan archivos chicos que degradan las lecturas | Alta | Medio | Partición por fecha (ADR-003), `coalesce` y compactación periódica de Bronze |
| R-6 | Spikes de costo: hasta 317 USD contra un p99 de 16,7 | Media | Medio | Flag de anomalía por z-score o MAD. No se eliminan: pueden ser reales |
| R-7 | Signo invertido en billing: 13 facturas | Media | Alto | Corrección condicionada al ratio de impuesto (ADR-006); si no lo cumple, quarantine |
| R-8 | AstraDB no disponible el día de la demo | Media | Alto | Capturas de una corrida previa y Cassandra en Docker como plan B |
| R-9 | PII en `users.email` | Baja | Alto | No se promueve a Gold: en Silver queda el hash y el dominio |
| R-10 | `value` viene como texto: castear sin control convierte a nulo en silencio | Baja | Alto | Cast con fallback a quarantine con el valor original |

## Decisiones abiertas

| # | Decisión | Bloquea | Responsable | Fecha |
|---|---|---|---|---|
| D-1 | ¿`credits` nulo es $0 o dato faltante? | Revenue neto del mart de FinOps | Garcia | 05/10/2026 |
| D-2 | ¿Cuál es la escala de CSAT y qué significa el 0? Va de 0 a 7, sin documentar | Métrica de satisfacción de Soporte | Lombardo | 05/10/2026 |
| D-3 | ¿Los `cost_usd_increment` negativos son ajustes o errores? | Regla de calidad del stream | Garcia | 15/10/2026 |
| D-4 | Grano exacto de cada mart Gold | Modelado de las tablas de serving | Morantes | 20/10/2026 |
| D-5 | Ventana de watermark del streaming | Agregación incremental de Silver a Gold | Morantes | 01/11/2026 |

## Roles

Los roles marcan quién lleva la decisión final de cada área, no quién toca el código: todo se
revisa por Pull Request cruzado para que nadie quede como único punto de conocimiento (R-8).

| Integrante | Lidera | Respalda |
|---|---|---|
| Olivia Garcia | Ingesta Bronze y Structured Streaming | Performance y compactación |
| Abril Lombardo | Silver, features y marts Gold | Documentación |
| Agustín Morantes | Calidad, quarantine, serving en Cassandra e infraestructura | Pruebas y evidencias |

La arquitectura, las decisiones técnicas y la defensa las llevamos entre los tres.

## Esfuerzo

En horas-persona. La primera entrega llevó 44 h contra 39 h estimadas; las estimaciones que
siguen incluyen ese margen.

| # | Tarea (segunda entrega) | Estimación | Responsable |
|---|---|---|---|
| 1 | Correcciones del feedback | 4–6 h | Equipo |
| 2 | Ingesta batch Landing a Bronze | 8–12 h | Garcia |
| 3 | Ingesta streaming con watermark, dedupe y checkpoint | 12–18 h | Garcia |
| 4 | Reglas de calidad y quarantine | 8–10 h | Morantes |
| 5 | Bronze a Silver: normalización, joins y compatibilidad v1/v2 | 10–14 h | Lombardo |
| 6 | Features de uso y costo | 6–8 h | Lombardo |
| 7 | Mart Gold de FinOps | 6–8 h | Lombardo |
| 8 | Keyspace en AstraDB y carga desde Spark | 8–12 h | Morantes |
| 9 | Pruebas y evidencias de ejecución | 8–10 h | Morantes |
| 10 | Diagrama v2 y documentación | 6–8 h | Equipo |
| | **Total** | **76–106 h** | |

Ruta crítica: 2, 3, 5, 6, 7, 8. El streaming es la tarea de mayor incertidumbre, así que arranca
temprano. El reparto queda en 23–35 h para Garcia, 25–35 h para Lombardo y 27–37 h para Morantes,
contando la parte proporcional de las tareas de equipo: entre 4 y 5 h semanales cada uno.

La entrega final suma otras 62–84 h con un reparto análogo, pero en tres semanas en vez de siete:
son 7 a 9 h semanales por persona. Para descomprimirla adelantamos a la segunda entrega todo lo
que no dependa del feedback, en particular el gobierno y el diccionario de datos.

## Recursos

Google Colab para ejecutar PySpark, AstraDB free tier para el serving, Docker con Cassandra local
como plan B, GitHub, Google Drive para los checkpoints entre sesiones y draw.io para el diagrama.
Todo gratuito, y eso es una restricción de diseño: por eso el dataset de demo se mantiene
reducido y el pipeline se puede correr por pasos en vez de una sola corrida larga.

## Próximos pasos

1. Convertir el feedback en un plan de correcciones versionado, con responsable y fecha objetivo.
2. Cerrar D-1 y D-2 con el docente: desbloquean las fórmulas de dos marts.
3. Congelar el esquema explícito de las 8 fuentes en `config/`, para no depender de `inferSchema`.
4. Prototipar el streaming con 2 o 3 archivos JSONL antes de escalarlo.
