WITH base_calculo AS (
SELECT
CASE 
{control}
FROM {zonatabla} 
WHERE {filtro}) 
SELECT IF(SUM(aux_control)> 0, 1, 0) AS var_control
FROM base_calculo;
--DETALLE
SELECT
{control}
FROM {zonatabla} 
WHERE {filtro};
--VARIACION
SELECT {fa},
{control}
FROM {zonatabla} 
WHERE {filtro_p}
GROUP BY 1
ORDER BY 1 DESC;