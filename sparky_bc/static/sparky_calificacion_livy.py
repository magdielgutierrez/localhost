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
import base64
import traceback
import os
def log(text, hdfs_path):
    Path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path
    FileSystem = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem

    # create FileSystem and Path objects
    hadoopConfiguration = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopFs = FileSystem.get(hadoopConfiguration)
    filePath = Path(hdfs_path + "/sparky.log")

    # create datastream and write out file
    dataStream = hadoopFs.create(filePath)
    dataStream.writeBytes(str(text).encode("utf-8"))
    dataStream.close()


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
    config = sys.argv[1] # 1 : json con la configuracion
    hdfs_path = sys.argv[2] # 2 : hdfs path
    encode = sys.argv[3]

    try:
        envi = os.environ["NM_HOST"]

        # Diccionario que mapea los entornos a sus rutas S3
        entornos_rutas = {
            "dev": "s3a://cdp-bcl-dev/datos/user/hive/warehouse/",
            "qa": "s3a://cdp-bcl-qa/datos/user/hive/warehouse/",
            "prd": "s3a://cdp-bcl-prd/datos/user/hive/warehouse/"
        }

        # Determinar la ruta S3 basada en el entorno
        ruta_s3 = next((ruta for key, ruta in entornos_rutas.items() if key in envi), None)

        if ruta_s3 is None:
            # Manejar el caso en que no se encuentra una ruta correspondiente
            # Puedes lanzar una excepción o asignar una ruta predeterminada
            raise ValueError("Entorno no reconocido en NM_HOST")
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
            df.write.partitionBy(config['partitionBy']).format("hive").option("fileFormat","parquet").option("path", ruta_s3 + nombre_tabla).saveAsTable(nombre_tabla, mode=config['mode'])
        else:
            df.write.format("hive").option("fileFormat","parquet").option("path", ruta_s3 + nombre_tabla).saveAsTable(nombre_tabla, mode=config['mode'])


        print("\nel proceso ha terminado, los resultados se encuentran en {}".format(config['tabla_dest']))

    except Exception as e:
        err = traceback.format_exc()
        log(err, hdfs_path)
        raise e

