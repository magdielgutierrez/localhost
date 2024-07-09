import pyodbc
import time
import pandas as pd
import warnings
from os import system

from dotenv import dotenv_values


config = dotenv_values(".env.development")
warnings.filterwarnings("ignore")
pyodbc.autocommit = True

dsn, host,port,user,pwd=config['dsn'],config['host'],config['port'],config['user'],config['pwd']

def cnxn_hue():

        cnxnstr= f"DSN={dsn};Server={host};Port={port};UID={user};PWD={pwd}"
        return pyodbc.connect(cnxnstr,autocommit=True)
    
if __name__ == '__main__':
        _=system('cls')
        # Master data    
        #.txn_plazo_fijo                    
        tbExportList=['pre_pagos_export_00']
        
        chunk_size = 600000
        print("\n ","-"*82)
        for table in tbExportList:
                
                try:
                        sqlExportQuery=f"SELECT * FROM proceso_bipa_vsc_appcorp.{table}"
                        dfs_extract = pd.read_sql(sqlExportQuery,cnxn_hue(),coerce_float=False)
                except:
                        sqlExportQuery=f"SELECT * FROM resultados_bipa_vsc_appcorp.{table}"
                        dfs_extract = pd.read_sql(sqlExportQuery,cnxn_hue(),coerce_float=False)
                        
  
                try:
                        dfs_extract.drop(columns = ['ingestion_year','ingestion_month','ingestion_day','year','month'],inplace=True)
                except:
                        print("Error en quitar columnsa que no existen")
                finally:
                        for i, g in dfs_extract.groupby(lambda x: x//chunk_size):
                                g.to_csv(f"./export/{table}_part_{i:02d}.txt", index=None, sep='|', mode='w')
                                print(f"|\n|      File generado ----> {table}_part_{i:02d}.txt | {g.shape[0]} rows")
                                time.sleep(1)
        print("\n ","-"*82)
        print("-"*82)
        print("|                        EJECUCION FINALIZADA EXITOSAMENTE                        |")
        print("-"*82)

   