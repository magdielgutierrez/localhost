INSERT INTO {zona_p}.{indice}_ingestion
SELECT 
    CAST({id_tabla} AS INT) AS id_tabla,
    "{tabla}" AS tabla,
    CAST({campo1} AS INT) AS campo1,
    CAST({campo2} AS INT) AS campo2,
    CAST({campo3} AS INT) AS campo3
FROM {zona}.{tabla}
WHERE {campo1}*10000+{campo2}*100+{campo3}<={fecha_num}
GROUP BY 1,2,3,4,5
ORDER BY 1 DESC, 2 DESC, 3 DESC, 4 DESC, 5 DESC
LIMIT 1