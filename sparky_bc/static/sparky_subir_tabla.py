# -*- coding: utf8 -*- 

"""
#PARA CALIFICAR A LOS CLIENTES CON ESTE CODIGO SE DEBE EJECUTAR EL SIGUIENTE COMANDO EN TERMINAL (Desde el servidor):
ssh -o ServerAliveInterval=5 sbmdeblze003.bancolombia.corp
spark-submit Calificacion/calificacion.py "/user/mpaz/Modelos_V1_newparams/modelos_g_sin_exp_nei" "proceso_clientes_personas_y_pymes.osef_consolidado_clean" "proceso_clientes_personas_y_pymes.osef_g_sin_exp_nei"
"""

# Importamos librerias utilizadas
from pyspark           import SparkContext, SparkConf
from pyspark.sql       import SQLContext, SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, LongType, BooleanType, TimestampType
import pandas          as pd
import unicodedata
import json
import sys
import re

def strip_accents(text):
    try:
        text = unicode(text, 'utf-8')
    except (TypeError, NameError): # unicode is a default on python 3 
        pass
    text = unicodedata.normalize('NFD', text)
    text = text.encode('ascii', 'ignore')
    text = text.decode("utf-8")
    return str(text)

def get_df(ruta, tipo = 'csv', **kwargs):
    """
    Metodo usado para obtener el dataframe
    """
    assert tipo in ['csv','excel','parquet'], "El tipo debe de ser o 'csv' , 'excel' o 'parquet'"
    if tipo == 'csv':
        return spark.read.csv(ruta, **kwargs)
    elif tipo == 'excel':
        df = pd.read_excel(ruta, **kwargs)

        tipos = {
            'O': StringType,
            'i': LongType,
            'b': BooleanType,
            'M': TimestampType,
            'f': DoubleType
        }

        col_types = df.dtypes.apply(lambda x: tipos[x.kind])

        schema = StructType([
            StructField(nombre, tipo(), True) for nombre, tipo in zip(df.columns, col_types)
        ])
        
        df = df.replace({pd.np.nan: None}) #Cambiar los nan por nulos
        return spark.createDataFrame(df, schema=schema)
    
    elif tipo == 'parquet':
        return spark.read.parquet(ruta, **kwargs)


if __name__ == '__main__':
    # Configuracion del ambiente de trabajo
    conf       = SparkConf().setAppName('subir_tabla') # Asigna el nombre del ambiente de trabajo
    sc         = SparkContext(conf=conf).getOrCreate()
    sc.setLogLevel('WARN')
    sqlcontext = SQLContext(sc)
    sqlcontext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    sqlcontext.setConf("hive.exec.dynamic.partition", "true")
    spark      = sqlcontext.sparkSession
    sql        = sqlcontext.sql

    # carga de configuracion
    csv_config = sys.argv[1] # se asignan los parametros necesarios para la ejecucion
    # 1 : json con la configuracion
    config = json.loads(csv_config)

    print('Leyendo tabla')
    print('info_orquestador: (leyendo)')
    df = get_df(config['ruta_origen'], config['tipo'], **config['kwargs'])

    # Limpieza en el nombre de las variables
    columns = [re.sub(r'[() \[\]]','_', x.strip()) for x in df.columns]
    columns = [strip_accents(x) for x in columns]
    columns = [re.sub(r'[^A-Za-z0-9_]','',x) for x in columns]
    df = df.toDF(*columns)

    # Compactar archivos pequeños
    df = df.coalesce(1)

    # Escritura de resultados en la LZ
    print('Escribiendo en la LZ')
    print('info_orquestador: (subir_lz)')
    nombre_tabla = config['nombre_tabla'] if re.search(r'\.', config['nombre_tabla']) else config['zona']+'.'+config['nombre_tabla']

    if config['partitionBy'] is not None:
        df.write.partitionBy(config['partitionBy']).format("hive").option("fileFormat","parquet").saveAsTable(nombre_tabla, mode=config['mode'])
    else:
        df.write.format("hive").option("fileFormat","parquet").saveAsTable(nombre_tabla, mode=config['mode'])



    print("\nel proceso ha terminado, los resultados se encuentran en {}".format(config['nombre_tabla']))


