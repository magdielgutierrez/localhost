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
    path_doc="./tables/0/"
    
    # read PDF file
    tables = tabula.io.read_pdf(path_doc + "dict_0.pdf", pages='all',
                         encoding="utf-8",lattice=False,stream=True, silent=True)
    
   
    tbl_cont=1
    for tbl in tables:
        #print(tbl)
        tbl_pivot=_formart_table(tbl)          
        dfs_tbl=pd.concat([dfs_tbl,tbl_pivot])

    # dfs_tbl[start:end].to_excel(f"{path_doc}tables_{tbl_cont}.xlsx", index=False)
     
    block=10
    for start in range(0,dfs_tbl.shape[0],10):
        end=start + block
        # print(start,end)
       
        print(f"    File generado ----> {path_doc}tables_{tbl_cont}.xlsx")  
        dfs_tbl[start:end].to_excel(f"{path_doc}tables_{tbl_cont}.xlsx", index=False)  
        tbl_cont+=1
    print()
    print("+-----------------------------------------------------------------+")
    print("|                EJECUCION FINALIZADA EXITOSAMENTE                |")
    print("+-----------------------------------------------------------------+")
    
   