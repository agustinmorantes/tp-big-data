# Configuración

- `pipeline.toml`: rutas, Spark, streaming, calidad, anomalías, Gold y serving. Está en el repo
  y no tiene secretos.
- `astra.env.example`: plantilla para las credenciales de AstraDB. Los valores reales van en
  `config/astra.env`, junto con el bundle `secure-connect-*.zip`. Los dos están en el `.gitignore`.

```bash
cp config/astra.env.example config/astra.env
```

Si un valor está en más de un lugar, gana la variable de entorno, después `config/astra.env` y
por último `pipeline.toml`. Para usar otro archivo de configuración:
`python -m cpa.run_pipeline --config <ruta> ...` o la variable `CPA_CONFIG`.

Los esquemas de las 8 fuentes están en `src/cpa/schemas.py`, como DDL de Spark.
