select {control} 
from {zonatabla} 
where {filtro};
--DETALLE
SELECT {control}
from {zonatabla}
where {filtro};
--VARIACION
SELECT {fa}, {control}
from {zonatabla}
where {filtro_p}
GROUP BY 1
ORDER BY 1 DESC;