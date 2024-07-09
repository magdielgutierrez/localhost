WITH base_calculo AS (
SELECT
CASE {control}
FROM {zonatabla}
WHERE {filtro}
)
SELECT IF(SUM(aux_control) > 0, 1, 0) AS var_control
FROM base_calculo;
--DETALLE
WITH base_calculo AS (
SELECT
CASE {control}
FROM {zonatabla}
WHERE {filtro}
)
SELECT SUM(aux_control) AS var_control
FROM base_calculo;
--VARIACION
WITH base_calculo AS (
SELECT {fa},
CASE {control}
FROM {zonatabla}
WHERE {filtro_p}
)
SELECT fa, SUM(aux_control) AS var_control
FROM base_calculo
GROUP BY 1
ORDER BY 1 DESC;