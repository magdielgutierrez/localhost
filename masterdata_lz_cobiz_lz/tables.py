import tabula
import os
import numpy as np
import pandas as pd

   

def _formart_table(tablondfs):
    dfs=tablondfs

    
    # LLenar columna con valor de la anterior
    dfs[['Tabla','Descripción']] = dfs[['Tabla','Descripción' ]].fillna(method='ffill')   
   
    dfs=dfs.groupby(['Tabla'], as_index=False).agg({'Descripción' : ' '.join, 'Descripción' : 'first'})
  
    return dfs
    


    

if __name__ == '__main__':
    
    dfs_tbl = pd.DataFrame()
    path_doc="./"
    
    # read PDF file
    tables = tabula.io.read_pdf(path_doc + "ex_dato_custodia.pdf", pages='all',
                         encoding="utf-8",lattice=False,stream=True, silent=True)

    
  
   
    tbl_cont=1
    for tbl in tables:     
        dfs_tbl=pd.concat([dfs_tbl,tbl])
        
    dfs_tbl_columns= dfs_tbl["Nombre Columna"]
    dfs_tbl_columns.dropna(inplace=True)
    dfs_tbl_columns.reset_index(inplace=True, drop=True) 
    print() 
    print("Listado de nombre de columnas en cadena de texto")    
    text = ', '.join(dfs_tbl_columns)
    print(text)

    print()
    print("+-----------------------------------------------------------------+")
    print("|                EJECUCION FINALIZADA EXITOSAMENTE                |")
    print("+-----------------------------------------------------------------+")
    
   