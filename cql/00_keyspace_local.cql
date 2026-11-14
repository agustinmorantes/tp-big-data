-- Solo para Cassandra local (Docker, plan B). En AstraDB el keyspace se crea desde la consola
-- web (Astra no permite CREATE KEYSPACE por CQL) con el nombre cloud_provider_analytics.
CREATE KEYSPACE IF NOT EXISTS cloud_provider_analytics
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
