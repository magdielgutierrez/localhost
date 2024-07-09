import pyodbc
import time
import warnings
import pandas as pd
import numpy as np
from datetime import datetime
from os import system
from dotenv import dotenv_values


config = dotenv_values(".env.development")
warnings.filterwarnings("ignore")
pyodbc.autocommit = True

dsn, host,port,user,pwd=config['dsn'],config['host'],config['port'],config['user'],config['pwd']

def cnxn_hue():
    cnxnstr= f"DSN={dsn};Server={host};Port={port};UID={user};PWD={pwd}"
    return pyodbc.connect(cnxnstr,autocommit=True)

def _info(flag):
        date=datetime.now()
        print(f"+{'-'*100}+") 
        print(f"|      \033[0;32;40m{date.strftime('%Y-%m-%d %H:%M:%S')} - [INFO] - {flag} \033[0m")
        print(f"+{'-'*100}+") 
        
if __name__ == '__main__':
        _=system('cls')
        zona='resultados_bipa_vsc_appcorp'
        
        _info('Inicializando exportacionn de tablas de la zona de resultado')
        lstTables=pd.read_excel('./db/Genshim_insumos_operativa_dev.xlsx', skiprows=1)
        lstTables=lstTables[(lstTables['RELEASE']=='R2') & (lstTables['DESTINO'] != 'COBIS')]
        lstTables=lstTables[['LIBRERÍA/PROCESO','NOMBRE_DEV_LZ','NOMBRE_PROD','SEPARADOR','DESTINO']]
        lstTables['NOMBRE_PROD']=lstTables['NOMBRE_PROD'].str.strip()
      
        
        exportList= lstTables['NOMBRE_DEV_LZ'].tolist()
        namesList= lstTables['NOMBRE_PROD'].tolist()
        typesList= lstTables['SEPARADOR'].tolist()
        destnoList= lstTables['DESTINO'].tolist()
        
        for table, name, type, destino in zip(exportList,namesList,typesList,destnoList):
            
            sqlExportQuery=f"SELECT * FROM {zona}.{table.lower()}"
            #dfs_extract = pd.read_sql(sqlExportQuery,cnxn_hue(), coerce_float=False)
            tabla=zona + '.' + table.lower()
            
            dfs_extract = pd.read_sql_table(tabla,cnxn_hue(), coerce_float=False)
            try:
                dfs_extract.drop(columns = ['ingestion_year','ingestion_month','ingestion_day','year','month'],inplace=True)
            except:
                print("Error en quitar columnsa que noe xisten")
            finally:   
                if type == 'pipe':
                    dfs_extract.to_csv(f"./export/{destino}/{name}",  index=None, header=None, sep='|', mode='w',encoding='utf-8')
                else:
                    np.savetxt(f"./export/{destino}/{name}", dfs_extract.values,fmt='%s',delimiter='',encoding='UTF-8')
                    
                _info(f"File generado ----> {name} | {dfs_extract.shape[0]} rows")
                        
       
        _info('Proceso termina satifactoriamente.')

   
   