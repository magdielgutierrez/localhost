drop table if exists {zona_p}.{indice}_ingestion purge;

create table if not exists {zona_p}.{indice}_ingestion
(
id_tabla int,
tabla string,
campo1 int,
campo2 int,
campo3 int
)
stored as parquet
;
