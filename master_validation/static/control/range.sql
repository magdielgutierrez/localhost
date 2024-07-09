WITH base_calculo AS (
SELECT {control}
FROM {zonatabla}
WHERE {filtro}
)
SELECT IF(SUM(aux_control) = COUNT(*), 0, 1) AS var_control FROM base_calculo;
--DETALLE
WITH base_calculo AS (
SELECT {control}
FROM {zonatabla}
WHERE {filtro}
)
SELECT (COUNT(*) - SUM(aux_control)) AS var_control FROM base_calculo;
--VARIACION
WITH base_calculo AS (
SELECT {fa}, {control}
FROM {zonatabla}
WHERE {filtro_p}
)
SELECT fa, (COUNT(*) - SUM(aux_control)) AS var_control 
FROM base_calculo
GROUP BY 1
ORDER BY 1 DESC;