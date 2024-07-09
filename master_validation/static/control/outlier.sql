WITH rangos AS (
  SELECT
    {control}
  FROM
    {zonatabla}
  WHERE
    {filtro}
)
SELECT
    IF(SUM(IF({nom_columna} BETWEEN r.l_range AND r.r_range,0,1))>0,1,0) as var_control
FROM
  {zonatabla},
  rangos r
WHERE
  {filtro};

--DETALLE
WITH rangos AS (
  SELECT
    {control}
  FROM
    {zonatabla}
  WHERE
    {filtro}
)
SELECT
    SUM(IF({nom_columna} BETWEEN r.l_range AND r.r_range,0,1)) as var_control
FROM
  {zonatabla},
  rangos r
WHERE
  {filtro};

--VARIACION
WITH rangos AS (
    SELECT
        year*100+month as fa,
        {control}
    FROM
        {zonatabla}
    WHERE
        {filtro_p}    
    group by 1
),
valores AS (
    SELECT
        year*100+month as fa,
        {nom_columna} as number_val
    FROM
        {zonatabla}
    WHERE
        {filtro_p}
)
SELECT
    t.fa,
    SUM(IF(t.number_val BETWEEN r.l_range AND r.r_range,0,1)) as var_control
FROM
  valores t
LEFT JOIN rangos r ON t.fa = r.fa
GROUP BY 1
ORDER BY 1 DESC