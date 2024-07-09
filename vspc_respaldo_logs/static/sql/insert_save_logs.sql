INSERT INTO {tabla} PARTITION (year)
SELECT CAST('{rutina}' AS STRING) AS rutina,
    CAST('{ejecutor}' AS STRING) AS ejecutor,
    CAST('{ip}' AS STRING) AS ip,
    CAST('{fecha_inicio}' AS TIMESTAMP) AS fecha_inicio,
    CAST('{fecha_fin}' AS TIMESTAMP) AS fecha_fin,
    CAST('{tiempo_minutos}' AS DOUBLE) AS tiempo_minutos,
    CAST('{estado}' AS STRING) AS estado,
    CAST('{error}' AS STRING) AS error,
    CAST('{log_name}' AS STRING) AS log_name,
    CAST('{log_file}' AS STRING) AS log_file,
    CAST('{year}' AS INT) AS year;

COMPUTE STATS {tabla};