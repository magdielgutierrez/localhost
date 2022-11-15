import tabula
import os
import numpy as np
import pandas as pd


def _change_type_default(col_type):
    lngtd=""
    
    if col_type=='INT':
        return '10'
        
    elif col_type=='DATETIME':
        return '29'
    
    elif col_type=='SMALLINT':
        return '6'
    
    elif col_type=='TINYINT':
        return '4'
    
    else:
        pass

 
    
def _change_value_datetime(col_date):
    format_date="yyyy-mm-dd hh:mm:ss"
    if col_date=='DATETIME':
        return format_date
    else:
        return ""
    
def _required_is_true(col_value):
    
    if col_value.upper()=='X':
        return "SI"
    else:
        
        return "NO"
    
def _value_is_null(col_value):
   
    if col_value.upper()=='SI':
        return "NULL"
    else:
        return ""



def _value_pk(col_value):
   
    if col_value.upper()=='NO':
        return "SI"
    else:
        return "NO"
    
# def _fill_nan_by_column(col_campo, col_requerido):
#    # print(type(col_campo),col_campo,"----", type(col_requerido), col_requerido)
    
#     if type(col_campo) == str and type(col_requerido)== str:
#         return "X"
    
        
   
    
    
def _only_int(col_text):
    if col_text != "":
        try:
            return  int(col_text)
        except:
            return " "
    else:
         return " "
    

def _formart_table(tablondfs):
    dfs=tablondfs
    
    #print(dfs.columns)
    dfs.rename(columns={"Nombre Columna": "Campo","Requerido":"COL_N"}, inplace=True)
    
    # LLenar columna con valor de la anterior
    dfs[['Campo','Tipo','COL_N']] = dfs[['Campo','Tipo','COL_N']].fillna(method='ffill')
    
    
    
    # dfs['Categoria_ID'] = dfs['COL_N'].apply(lambda x: 1 if type(x)==str else np.Na)
    # # for row in range(dfs.shape[0]):
    # #     col_requerido=dfs['COL_N'][row]
    # #     col_campo=dfs['Campo'][row]
    # #     answer=_fill_nan_by_column(col_campo,col_requerido)
    # #     print(answer)
    # #     dfs['demo'][row]=answer
    
   
    
    
    if "Unnamed: 1" in dfs.columns and "Unnamed: 0" in dfs.columns:
        dfs['Contents']=   (dfs['Unnamed: 0'] + ' '+ dfs['Unnamed: 1']).fillna("").astype(str)
        dfs['Contents']= dfs['Descripción'].astype(str) + ' '+dfs['Contents'].astype(str)
        dfs.drop(['Unnamed: 1','Unnamed: 0'], axis=1, inplace=True)
    elif "Unnamed: 0" in dfs.columns:
        print("entramos en el IF")
        dfs['Contents']=   dfs['Unnamed: 0'].fillna("").astype(str)
        dfs['Contents']= dfs['Descripción'].astype(str) + ' ' +dfs['Contents'].astype(str)
        dfs.drop(['Unnamed: 0'], axis=1, inplace=True)
    else:
        dfs['Contents']= dfs['Descripción'].astype(str)
  
  
   
    dfs=dfs.groupby(['Campo','Tipo','COL_N'], as_index=False).agg({'Contents' : ' '.join, 'Descripción' : 'first'}) 
    dfs.drop(['Descripción'], axis=1, inplace=True)
    #print(dfs['COL_N'])
    
    # format column tipo de dato
    dfs['Tipo']=dfs['Tipo'].str.upper()
    dfs['COL_J']= dfs['Tipo'].str.extract(r'(\w+)')

    # procces extrac longitud de campo
    dfs['pvt_text']=dfs['Tipo'].str.extract(r'(\d+\,?\d*)').fillna("")
  
    
    # procees para definir el tipo de dato decimal
    dfs['decimal']=dfs['pvt_text'].str.extract(r'(\d+\,\d)').fillna("")
    dfs['COL_L']=dfs['decimal'].str.extract(r'(\d+)').fillna("")
    dfs['COL_M']=dfs['decimal'].str.extract(r'(\d$)').fillna("")
    
    
    dfs['COL_K_1']=dfs['pvt_text'].apply(lambda col_text:_only_int(col_text)).fillna("") 
    dfs['COL_K_2']=dfs['COL_J'].apply(lambda col1:_change_type_default(col1)).fillna("")

    
    dfs['COL_K']= dfs['COL_K_1'].astype(str) +  dfs['COL_K_2'].astype(str)
    dfs['COL_R']=dfs['COL_J'].apply(lambda col_date:_change_value_datetime(col_date))
    
    
    # Valor requerido si o no
    dfs['COL_N']= dfs['COL_N'].apply(lambda col_value:_required_is_true(col_value))
    
    # Valor por defecto
    dfs['COL_O']= dfs['COL_N'].apply(lambda col_value:_value_is_null(col_value))
    
    # Valor column PK
    dfs['COL_P']= dfs['COL_N'].apply(lambda col_value:_value_pk(col_value))
    
    
    # Columnas valores por defecto
    dfs['COL_C']=tbl_name
    
    dct_col_add={'COL_A':'OTROS' ,'COL_B':'Banistmo',
                'COL_D':'cobis' ,'COL_E':'cobis' ,'COL_F':'cobis',
                'COL_H':'','COL_Q':'NO'}

    for key,value in dct_col_add.items():
        dfs[key]=value

    # Renombrando ultimas columnas
    dfs.rename(columns={"Campo": "COL_G",'Contents':'COL_I', }, inplace=True)


    # Fill blank value
    dfs['COL_I'].replace("nan", 'No aplica',inplace=True)
    #print(dfs['COL_I'])
    # Ordenamiento_columnas
    order_cols=['COL_A','COL_B','COL_C','COL_D' ,'COL_E' ,'COL_F' ,'COL_G','COL_H','COL_I',
                'COL_J','COL_K','COL_L','COL_M','COL_N','COL_O','COL_P','COL_Q','COL_R']
    dfs = dfs[order_cols] 
  
    return dfs
    


    

if __name__ == '__main__':
    
    dfs_tbl = pd.DataFrame()
    page = input("|-----> Ingrese numero de folder: ")
    # page="2"
    path_doc=f"./tables/{page}/"
    
    # read PDF file
    tables = tabula.io.read_pdf(path_doc + f"dict_{page}.pdf", pages='all',
                         encoding="utf-8",lattice=False,stream=True, silent=True)
    

    tbl_cont=1
    for tbl in tables:
        print()
        print(f"------------------------------------ TABLA  # {tbl_cont} ------------------------------------+")
        print("  ",tbl.head(15))
        print()
        opn=input("|------->  Exportar table Y/N:  ").lower()
        
        if opn=='n':
            print()
            tbl_pivot=pd.DataFrame()
            _=input("|------->  continue...")
        else:
            tbl_name=input("|------->  Ingrese nombre de la tabla: ")
           # tbl_name="ddddd"
            tbl_pivot=_formart_table(tbl)
            print(f"|------->  TABLA {tbl_name} EXPORTADA EXITOSAMENTE ")   
            print()
            print("+---------------------------------------------------------------------------------------+")
            _=input("|------->  next...")
           
        
        dfs_tbl=pd.concat([dfs_tbl,tbl_pivot])
        #print(dfs_tbl.shape[0])
        os.system('cls')
        print()
        tbl_cont+=1
      
    print()
     
    
    # dfs_tbl['COL_K']=dfs_tbl['COL_K'].str.replace(' ','0').astype(int)  
    # dfs_tbl['COL_L']=dfs_tbl['COL_L'].str.replace('','0').astype(int)  
    # dfs_tbl['COL_M']=dfs_tbl['COL_M'].str.replace('','0').astype(int)
    dfs_tbl.to_excel(f"{path_doc}output_{page}.xlsx", index=False)
    print("+-----------------------------------------------------------------+")
    print("|                EJECUCION FINALIZADA EXITOSAMENTE                |")
    print("+-----------------------------------------------------------------+")
    print(f"File generado ----> {path_doc}output_{page}.xlsx")
   