import pyodbc
import pandas as pd
import warnings

from dotenv import dotenv_values


config = dotenv_values(".env.development")
warnings.filterwarnings("ignore")
pyodbc.autocommit = True

host,port,user,pwd=config['host'],config['port'],config['user'],config['pwd']

def cnxn_hue():

        cnxnstr= f"DSN={'Impala_Prod'};Server={host};Port={port};UID={user};PWD={pwd}"
        return pyodbc.connect(cnxnstr,autocommit=True)
    
if __name__ == '__main__':
    
    sqlquery="""SELECT nombres,apellidos,identificacion,ciudad_residencia 
                    FROM default.prueba_small_files LIMIT 20;"""
    
    dfs_extract = pd.read_sql(sqlquery,cnxn_hue())
    #print(dfs_extract)
    print("+-----------------------------------------------------------------+")
    tblname=input("|      Ingrese nombre de la tabla a guardar:")
    
    dfs_extract.to_csv(f"{tblname}.txt", header=None, index=None, sep='|', mode='w')
    
    print("+-----------------------------------------------------------------+")
    print("|                EJECUCION FINALIZADA EXITOSAMENTE                |")
    print(f"|      File generado ----> {tblname}.txt")
    print("+-----------------------------------------------------------------+")
