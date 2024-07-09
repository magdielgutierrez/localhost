# -*- coding: utf8 -*- 
from pyspark           import SparkContext, SparkConf
from pyspark.sql       import SQLContext
import sys
import json

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

    # ejecución de consultas
    config = json.loads(sys.argv[1])
    queries_list = config.get("queries", [])
    max_tries = int(config.get("max_tries", 1))
    
    for i, query in enumerate(queries_list):
        print("Ejecutando la consulta {} de {} : {}".format(i+1, len(queries_list), query))
        print("info_orquestador: ({}/{})".format(i+1, len(queries_list)))
        intentar(query.strip().strip(';'), max_tries, i)
    print("Se ejecutaron con éxito {} consultas".format(len(queries_list)))

