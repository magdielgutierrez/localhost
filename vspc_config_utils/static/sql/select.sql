WITH ult_f_ing AS 
    (SELECT
        ingestion_year,
        ingestion_month,
        ingestion_day,
        fecha_ejecucion
    FROM {zona}.{indice}_central_params
    ORDER BY 
        ingestion_year DESC,
        ingestion_month DESC,
        ingestion_day DESC,
        fecha_ejecucion DESC
    LIMIT 1)
select {columns_name} 
from {zona}.{indice}_central_params A
INNER JOIN ult_f_ing B 
ON 
    A.ingestion_year=B.ingestion_year AND
    A.ingestion_month=B.ingestion_month AND
    A.ingestion_day=B.ingestion_day AND
    A.fecha_ejecucion=B.fecha_ejecucion