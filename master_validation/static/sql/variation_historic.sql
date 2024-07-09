SELECT
    result_ctrl,
    ingestion_year,
    ingestion_month,
    ingestion_day,
    fecha_ejecucion,
    parametro_ctrl
FROM {zona_tabla} 
WHERE
    nombre_flujo = '{nombre_flujo}' AND
    etapa_ejecucion = '{etapa_ejecucion}' AND
    tabla = '{tabla}' {nom_columna} AND
    ctrl = '{ctrl}'
ORDER BY 
    ingestion_year DESC,
    ingestion_month DESC,
    ingestion_day DESC,
    fecha_ejecucion DESC
LIMIT {periodo}