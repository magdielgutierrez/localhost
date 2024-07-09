select {control} 
from {zonatabla} 
where {filtro};
--DETALLE
select {control} 
from {zonatabla} 
where {filtro};
--VARIACION
select {fa}, {control} 
from {zonatabla} 
where {filtro_p}
group by 1
order by 1 desc;
