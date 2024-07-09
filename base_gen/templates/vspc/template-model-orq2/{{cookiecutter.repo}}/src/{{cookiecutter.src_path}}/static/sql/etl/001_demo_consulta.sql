-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- {{cookiecutter.vp_max}}
-----------------------------------------------------------------------------
-- Fecha Creación: {{cookiecutter.creation_date}}
-- Última Fecha Modificación: {{cookiecutter.creation_date}}
-- Autores: {{cookiecutter.package_author}}
-- Últimos Autores: {{cookiecutter.package_author}}
-- Descripción: Depuración de las tablas temporales asociadas al paso
--              o a toda la rutina según aplique.
-----------------------------------------------------------------------------
---------------------------------- INSUMOS ----------------------------------
-- resultados_vspc_clientes.master_customer_data
--------------------------------- RESULTADOS --------------------------------
-- proceso.temporal_ads_package_gen
-----------------------------------------------------------------------------
-------------------------------- Query Start --------------------------------

-- ESTO ES SOLO UNA CONSULTA DE MUESTRA, SE DEBEN COLOCAR TODAS LAS CONSULTAS
-- Y PARÁMETROS ASOCIADOS A LA RUTINA
DROP TABLE IF EXISTS {zona_p}.{indice}_temporal_ads_package_gen PURGE;
CREATE TABLE IF NOT EXISTS {zona_p}.{indice}_temporal_ads_package_gen STORED AS PARQUET TBLPROPERTIES ('transactional'='false') AS
    SELECT
        tipo_doc,
		num_doc,
		p_nombre,
		ctrl_terc,
		nombre_ciudad_dirp
    FROM
        resultados_vspc_clientes.master_customer_data
    LIMIT
        10;
COMPUTE STATS {zona_p}.{indice}_temporal_ads_package_gen;
--------------------------------- Query End ---------------------------------