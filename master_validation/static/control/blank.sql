WITH base_calculo AS (
SELECT 
CASE WHEN REGEXP_LIKE(CAST({nom_columna} AS STRING), '\\s') THEN 1 ELSE 0 END AS aux_control
FROM {zonatabla}
WHERE {filtro}
)
SELECT {control}
FROM base_calculo;
--DETALLE
WITH base_calculo AS (
SELECT 
CASE WHEN REGEXP_LIKE(CAST({nom_columna} AS STRING), '\\s') THEN 1 ELSE 0 END AS aux_control
FROM {zonatabla}
WHERE {filtro}
)
SELECT {control}
FROM base_calculo;
--VARIACION
WITH base_calculo AS (
SELECT {fa},
CASE WHEN REGEXP_LIKE(CAST({nom_columna} AS STRING), '\\s') THEN 1 ELSE 0 END AS aux_control
FROM {zonatabla}
WHERE {filtro_p}
)
SELECT fa, {control}
FROM base_calculo
GROUP BY 1
ORDER BY 1 DESC;