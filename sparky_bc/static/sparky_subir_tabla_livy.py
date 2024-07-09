# coding: utf-8

from pyspark           import SparkContext, SparkConf
from pyspark.sql       import SQLContext, SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, LongType, BooleanType, TimestampType
import unicodedata
import json
import sys
import re
import logging
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

def strip_accents(text):
    try:
        text = unicode(text, 'utf-8')
    except (TypeError, NameError): # unicode is a default on python 3
        pass
    text = unicodedata.normalize('NFD', text)
    text = text.encode('ascii', 'ignore')
    text = text.decode("utf-8")
    return str(text)

def get_df(ruta, tipo = 'parquet', **kwargs):
    """
    Metodo usado para obtener el dataframe
    """
    assert tipo in ['parquet'], "El tipo debe de ser 'parquet'"

    if tipo == 'parquet':
        return spark.read.parquet(ruta)


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
        if encode=="1": config = base64.b64decode(config).decode()
        config = json.loads(config)

        print('Leyendo tabla')
        print('info_orquestador: (leyendo)')

        df = get_df(config['ruta_origen'], config['tipo'], **config.get('kwargs',{}))

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
            df.write.partitionBy(config['partitionBy']).format("hive").option("fileFormat","parquet").option("path", ruta_s3 + nombre_tabla).saveAsTable(nombre_tabla, mode=config['mode'])
        else:
            df.write.format("hive").option("fileFormat","parquet").option("path", ruta_s3 + nombre_tabla).saveAsTable(nombre_tabla, mode=config['mode'])

        print("\nel proceso ha terminado, los resultados se encuentran en {}".format(config['nombre_tabla']))

    except Exception as e:
        err = traceback.format_exc()
        log(err, hdfs_path)
        raise e
