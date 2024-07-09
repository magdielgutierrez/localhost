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
order by anio_finalizacion

)
select 
IF(ultima_ingestion = fecha_val, 0, 1) AS val_ingestion
from ult_fi 
order by val_ingestion asc
limit 1
;
--DETALLE
select
concat(cast(anio_finalizacion as string),
cast(mes_finalizacion as string),
cast(dia_finalizacion as string)) as ultima_ingestion
from resultados.reporte_flujos_oozie
where anio_finalizacion = year(now()) and anio_finalizacion = year(date_sub(now(), 1)) 
and trim(lower(nombre_flujo)) = '{ozie}'
and trim(lower(estado_finalizacion)) = 'succeeded'
order by anio_finalizacion desc, mes_finalizacion desc, dia_finalizacion desc
limit 1
;