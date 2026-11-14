# Datos

El dataset lo provee la cátedra y no se versiona (`.gitignore`). Descomprimirlo para que quede:

```
data/datalake/landing/
  customers_orgs.csv  users.csv  resources.csv  billing_monthly.csv
  support_tickets.csv  marketing_touches.csv  nps_surveys.csv
  usage_events_stream/events_part_0000.jsonl … events_part_0119.jsonl
```

El pipeline escribe Bronze, Silver, Gold, quarantine, checkpoints y métricas en
`data/datalake/` junto a `landing/`. `python -m cpa.run_pipeline reset` los borra sin tocar
Landing. Para pruebas rápidas hay fixtures mínimos en `tests/fixtures/landing/`.
