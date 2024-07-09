WITH base_calculo AS (
SELECT
CASE 
WHEN REGEXP_LIKE(TRIM(CAST({nom_columna} AS STRING)), '^[0-9]+(\.[0-9]+)?$') OR {nom_columna} is NULL THEN 0 ELSE 1 END AS aux_control
FROM {zonatabla} 
WHERE {filtro}) 
SELECT {control}
FROM base_calculo;
--DETALLE
WITH base_calculo AS (
SELECT
CASE 
WHEN REGEXP_LIKE(TRIM(CAST({nom_columna} AS STRING)), '^[0-9]+(\.[0-9]+)?$') OR {nom_columna} is NULL THEN 0 ELSE 1 END AS aux_control
FROM {zonatabla} 
WHERE {filtro}) 
SELECT {control}
FROM base_calculo;
--VARIACION
WITH base_calculo AS (
SELECT {fa},
CASE 
WHEN REGEXP_LIKE(TRIM(CAST({nom_columna} AS STRING)), '^[0-9]+(\.[0-9]+)?$') OR {nom_columna} is NULL THEN 0 ELSE 1 END AS aux_control
FROM {zonatabla} 
WHERE {filtro_p}) 
SELECT fa, {control}
FROM base_calculo
GROUP BY 1
ORDER BY 1 DESC;