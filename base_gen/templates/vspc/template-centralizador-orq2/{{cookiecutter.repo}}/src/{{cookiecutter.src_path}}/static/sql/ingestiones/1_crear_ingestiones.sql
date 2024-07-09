create table if not exists {zona_r}.{indice}_ingestion_hist
(
tabla string,
campo1 int,
campo2 int,
campo3 int
ingestion_day int,
ingestion_month int,
ingestion_year int
)
stored as parquet
;

COMPUTE STATS {zona_r}.{indice}_ingestion_hist
