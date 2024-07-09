SELECT {control} 
FROM {zonatabla}
WHERE {filtro};
--DETALLE
SELECT {control} 
FROM {zonatabla}
WHERE {filtro};
--VARIACION
SELECT {fa}, {control} 
FROM {zonatabla}
WHERE {filtro_p}
GROUP BY 1
ORDER BY 1 DESC;