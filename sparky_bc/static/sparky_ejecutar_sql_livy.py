# -*- coding: utf8 -*- 
from pyspark           import SparkContext, SparkConf
from pyspark.sql       import SQLContext
import sys
import json
import base64
import traceback

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

if __name__ == '__main__':
    # Configuracion del ambiente de trabajo
    conf       = SparkConf().setAppName('exec_sql') # Asigna el nombre del ambiente de trabajo
    sc         = SparkContext(conf=conf).getOrCreate()
    sc.setLogLevel('WARN')
    sqlcontext = SQLContext(sc)
    sqlcontext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    sqlcontext.setConf("hive.exec.dynamic.partition", "true")
    spark      = sqlcontext.sparkSession
    sql        = sqlcontext.sql

    def intentar(query, max_tries, i, tries=1):
        try:
            spark.sql(query)
        except Exception as e:
            if tries >= max_tries:
                print("Error en la consulta {} : {}".format(i+1, e))
                print("info_orquestador: (ERROR query {})".format(i+1))
                raise Exception(e)
            print("info_orquestador: (Retry {})".format(tries))
            print("Error al ejecutar consulta, iniciando intento n. {} : {}".format(tries+1, query))
            intentar(query, max_tries, i, tries+1)

    # carga de configuracion
    config = sys.argv[1] # 1 : json con la configuracion
    hdfs_path = sys.argv[2] # 2 : hdfs path
    encode = sys.argv[3]
        
    try:
        if encode=="1": config = base64.b64decode(config).decode()
        config = json.loads(config)

        queries_list = config.get("queries", [])
        max_tries = int(config.get("max_tries", 1))
        
        for i, query in enumerate(queries_list):
            print("Ejecutando la consulta {} de {} : {}".format(i+1, len(queries_list), query))
            print("info_orquestador: ({}/{})".format(i+1, len(queries_list)))
            intentar(query.strip().strip(';'), max_tries, i)
        print("Se ejecutaron con éxito {} consultas".format(len(queries_list)))
    except Exception as e:
        err = traceback.format_exc()        
        log(err, hdfs_path)        
        raise e
