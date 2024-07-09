with ult_fi as (
select
concat(cast(anio_finalizacion as string),
cast(mes_finalizacion as string),
cast(dia_finalizacion as string)) as ultima_ingestion
{control}
from resultados.reporte_flujos_oozie
where anio_finalizacion = year(now()) and anio_finalizacion = year(date_sub(now(), 1)) 
and trim(lower(nombre_flujo)) = '{ozie}'
and trim(lower(estado_finalizacion)) = 'succeeded'
group by ultima_ingestion
)
select 
IF(ultima_ingestion = fecha_val, 0, 1) AS val_ingestion
from ult_fi 
order by val_ingestion asc
limit 1
;
--DETALLE
SELECT concat(cast(anio_finalizacion as string),
                cast(mes_finalizacion as string),
                cast(dia_finalizacion as string)) as ultima_ingestion
FROM resultados.reporte_flujos_oozie
WHERE anio_finalizacion = year(now()) and anio_finalizacion = year(date_sub(now(), 1))
    and trim(lower(nombre_flujo)) = '{ozie}'
    and trim(lower(estado_finalizacion)) = 'succeeded' 
ORDER BY anio_finalizacion DESC, mes_finalizacion DESC, dia_finalizacion DESC
LIMIT 1;