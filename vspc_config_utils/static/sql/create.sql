CREATE TABLE IF NOT EXISTS {zona_r}.{tabla} (
ingestion_year INT,
ingestion_month INT,
ingestion_day INT,
fecha_ejecucion TIMESTAMP,
{campos_tipo}
) partitioned by (year INT)
stored as parquet;