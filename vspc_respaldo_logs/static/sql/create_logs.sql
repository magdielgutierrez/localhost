CREATE TABLE IF NOT EXISTS {}_hist (
ingestion_year INT,
ingestion_month INT,
ingestion_day INT,
i STRING,
n INT,
documento STRING,
tipo STRING,
nombre STRING,
estado STRING,
hora_inicio TIMESTAMP,
duracion STRING,
etapa STRING,
sub_i INT,
fecha_carga TIMESTAMP,
cant_registros STRING
) partitioned by (year INT)
stored as parquet;