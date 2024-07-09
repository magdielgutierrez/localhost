with ult_fi as (
select
concat(cast(ingestion_year as string),
cast(ingestion_month as string),
cast(ingestion_day as string)) as ultima_ingestion
{control}
from {zonatabla}
)
select 
IF(ultima_ingestion = fecha_val, 0, 1) AS val_ingestion
from ult_fi
order by val_ingestion asc
limit 1
;
--DETALLE
select
concat(cast(ingestion_year as string),
cast(ingestion_month as string),
cast(ingestion_day as string)) as ultima_ingestion
from {zonatabla}
order by ingestion_year desc, ingestion_month desc, ingestion_day desc
limit 1
;