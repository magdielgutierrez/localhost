import pyodbc
import time
import pandas as pd
import numpy as np
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
        tbExportList=['pasib_tdc_extra']

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
                        print("Error en quitar columnsa que noe xisten")
                finally:
                        #dfs_extract.to_csv(f"./export/{table}.txt", header=False, index=None, sep='|', mode='w',encoding='utf-8')
                        #dfs_extract.to_csv(f"./export/{table}.txt",  index=None, sep='|', mode='w',encoding='utf-8')
                        
                        # export txt separador con tabulacion
                        dfs_extract.to_csv(f"./export/{table}.txt",  index=None, sep='\t', mode='w',encoding='utf-8')
                        
                        #dfs_extract.to_excel(f"./export/{table}.xlsx",  index=None)
                        #np.savetxt(f"./export/{table}", dfs_extract.values,fmt='%s',delimiter='',encoding='UTF-8')

                print(f"|\n|      File generado ----> {table}.txt | {dfs_extract.shape[0]} rows")
        print("\n ","-"*82)
        print("-"*82)
        print("|                        EJECUCION FINALIZADA EXITOSAMENTE                        |")
        print("-"*82)

   