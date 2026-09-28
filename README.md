# Cloud Provider Analytics — 72.80 Big Data (ITBA)

Pipeline de ingesta, conformado y serving sobre los datos de un proveedor cloud, para analítica
de FinOps, Soporte y Producto.

Integrantes: Olivia Garcia (61071) · Abril Lombardo (63655) · Agustín Morantes (61306)

Primera entrega: diseño y fundación del repositorio. El pipeline se implementa a partir de la
segunda.

## Arquitectura

Patrón híbrido: batch para las 7 fuentes maestras, CRM y facturación; Structured Streaming para
`usage_events_stream`. Las dos rutas escriben en el mismo Data Lake, con zonas Landing (formato
original, inmutable), Bronze, Silver y Gold, en Parquet a partir de Bronze y particionado por
fecha. El detalle está en el informe de diseño y en `docs/decisiones.md`.

## Estructura

```
docs/           decisiones técnicas, plan inicial y diagrama de arquitectura
notebooks/      exploración y perfilado con PySpark, con las salidas guardadas
data/           el dataset descomprimido (ignorado por git)
src/            código del pipeline
tests/          pruebas de transformaciones y calidad
config/         configuración externalizada, sin credenciales
infra/          scripts de ejecución
```

El dataset lo provee la cátedra. Se descomprime dentro de `data/`, de forma que quede
`data/datalake/landing/` con los 7 CSV y los 120 archivos JSONL del stream. Su contenido está en
`.gitignore`: los datos no se versionan.

## Correr los notebooks

Requisitos: Python 3.10+, Java 17+ y PySpark 3.5+. En Colab ya vienen.

Abrir `notebooks/01_exploracion_fuentes.ipynb`, subir el `.zip` del dataset cuando lo pida y
ejecutar las celdas en orden. En local, `pip install -r requirements.txt`, descomprimir el dataset
en `data/` y abrir el mismo notebook; si quedó en otra ruta, cambiar `DATA_DIR` en la celda de
carga.

Los notebooks se commitean con las salidas guardadas: son la evidencia de cada entrega.
