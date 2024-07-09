insert into {zona_r}.{tabla} partition (year)
select
year(now()) as ingestion_year,
month(now()) as ingestion_month,
day(now()) as ingestion_day,
now() as fecha_ejecucion,
{campos},
year(now()) as year 
from {zona_p}.{tabla}; 
