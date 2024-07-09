# -*- coding: utf8 -*- 

"""
#PARA CALIFICAR A LOS CLIENTES CON ESTE CODIGO SE DEBE EJECUTAR EL SIGUIENTE COMANDO EN TERMINAL (Desde el servidor):
ssh -o ServerAliveInterval=5 sbmdeblze003.bancolombia.corp
spark-submit Calificacion/calificacion.py "/user/mpaz/Modelos_V1_newparams/modelos_g_sin_exp_nei" "proceso_clientes_personas_y_pymes.osef_consolidado_clean" "proceso_clientes_personas_y_pymes.osef_g_sin_exp_nei"
"""

# Importamos librerias utilizadas
from pyspark.ml                import PipelineModel
from pyspark                   import SparkContext, SparkConf
from pyspark.sql               import SQLContext
import json
import sys
import re

def formatear_col(match):
    """
    Funcion usada para una vez se tiene identificada la columna formatearla para pasarla a 
    la sentencia SQL
    """
    if not match.group(3):
        # Si los valores de la columna no son vectores
        formato = match.group(1)
        formato += (' as '+match.group(6)) if match.group(6) else ""
    else:
        # So los valores de la columna son vectores
        formato = "get_item({},{})".format(match.group(1),match.group(3))
        formato += ' as ' + (match.group(6) if match.group(6) else "{}_{}".format(match.group(1),match.group(3)))
    return formato

if __name__ == '__main__':
    # Configuracion del ambiente de trabajo
    conf       = SparkConf().setAppName('calificacion_sparky') # Asigna el nombre del ambiente de trabajo
    sc         = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    sqlcontext = SQLContext(sc)
    sqlcontext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    sqlcontext.setConf("hive.exec.dynamic.partition", "true")
    spark      = sqlcontext.sparkSession
    sql        = sqlcontext.sql
    
    # carga de configuracion
    json_config = sys.argv[1] # se asignan los parametros necesarios para la ejecucion
    # 1 : json con la configuracion
    config = json.loads(json_config)
    
    print(config['modelo'], config['tabla_orig'], config['tabla_dest'])
    print('cargando modelo {}'.format(config['modelo']))
    print('info_orquestador: (carga_mod)')
    modelo_objeto = PipelineModel.load(config['modelo'])  # carga del modelo

    print('modelo cargado, seleccionando datos desde {}'.format(config['tabla_orig']))
    print('info_orquestador: (carga_info)')
    sql("use {}".format(config['zona']))
    df = sql ("select * from {}".format(config['tabla_orig']))
    
    print('Calificando')
    print('info_orquestador: (calific)')
    df1 = modelo_objeto.transform(df)  # calificacion de los clientes con el modelo cargado
    
    # Escritura de resultados en la LZ
    print('Escribiendo en la LZ')
    print('info_orquestador: (subir_lz)')

    def get_item(column, index):
        return float(column[index])

    spark.udf.register('get_item',get_item,'double')

    if len(config['variables'])==0:
        columnas = '*'
    else:
        revisar_columnas = [re.match(r'(\w+)(\[(\w+)\])?((\s+as)?\s+(\w+))?$', x, re.IGNORECASE) for x in config['variables']]
        columnas_malas   = [x for x,y in zip(config['variables'],revisar_columnas) if not y]
        assert all(revisar_columnas), "Error en el formato de las columnas {}".format(columnas_malas)
        
        columnas = ',\n            '.join([formatear_col(x) for x in revisar_columnas])
    
    df1.createOrReplaceTempView("resultados_calificacion")
    df = sql(""" 
        select
            {}
        from resultados_calificacion
    """.format(columnas))
    nombre_tabla = config['tabla_dest'] if re.search(r'\.', config['tabla_dest']) else config['zona']+'.'+config['tabla_dest']

    if config['partitionBy'] is not None:
        df.write.partitionBy(config['partitionBy']).format("hive").option("fileFormat","parquet").saveAsTable(nombre_tabla, mode=config['mode'])
    else:
        df.write.format("hive").option("fileFormat","parquet").saveAsTable(nombre_tabla, mode=config['mode'])


    print("\nel proceso ha terminado, los resultados se encuentran en {}".format(config['tabla_dest']))


