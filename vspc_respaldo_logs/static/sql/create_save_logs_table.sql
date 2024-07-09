DROP TABLE IF EXISTS {tabla} PURGE;
--
CREATE TABLE {tabla} (
    {comparar_str}
)
PARTITIONED BY (year INT) STORED AS PARQUET TBLPROPERTIES ("transactional" = "false");