WITH base_calculo AS (
SELECT {control}
FROM {zonatabla}
WHERE {filtro}
)
SELECT IF(SUM(var_control) = COUNT(*), 0, 1) AS aux_control FROM base_calculo;
--DETALLE
WITH base_calculo AS (
SELECT {control}
FROM {zonatabla}
WHERE {filtro}
)
SELECT (COUNT(*)-SUM(var_control)) AS aux_control FROM base_calculo;
--VARIACION
WITH base_calculo AS (
SELECT {fa}, {control}
FROM {zonatabla}
WHERE {filtro_p}
)
SELECT fa, (COUNT(*)-SUM(var_control)) AS aux_control
FROM base_calculo
GROUP BY 1
ORDER BY 1 DESC;