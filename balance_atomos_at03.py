import math
import pandas as pd
import re
import warnings
import numpy as np
from os import system
from datetime import datetime
from dotenv import dotenv_values
from itertools import zip_longest
from itertools import groupby

warnings.filterwarnings("ignore")

# flags para columnas segun origen
ftxt01, nFilea = 'rcob', 'PASIB_AT03_TDC'
ftxt02, nFileb = 'rbnk_', 'lz_tdcredito'

pathTemp = 'tdc_abr'
fType = 'tdc'



# cOLUMNAS DE AT03
colsNames=["cl01fecha","cl02cod_banco","cl03subsidiaria","cl04preferencial","cl05feci","cl06tipo_cred",
"cl07fac_cred","cl08clas_pre","cl09destino","cl10cod_región","cl11id_clien","cl12tam_empre","cl13gener",
"cl14num_cta","cl15nom_clie","cl16grp_econom","cl17tipo_rela","cl18actividad",
"cl19tasa ","cl20valor_ini ","cl21int_cobrar","cl22fecha_ini","cl23fecha_ven","cl24fecha_ref","cl25fecha_rest ",
"cl26fac_anterior","cl27primera_gar","cl28valor_prim_gar","cl29segunda_gar","cl30valor_segunda_gar","cl31tercera_gar",
"cl32valor_tercera_gar","cl33cuarta_gar","cl34valor_cuarta_gar","cl35quinta_gar","cl36valor_quinta_gar","cl37provision",
"cl38provision_niff","cl39prov_com_no_niff","cl40Saldo_","cl41cuota_xv","cl42mto_xV30d","cl43mto_xV60d",
"cl44mto_xV90d","cl45mto_xV120d","cl46mto_xV180d","cl47mto_xV1a","cl48mto_xV1a5a","cl49mto_xV5a10a",
"cl50mto_xVm10a","cl51cuotas_v","cl52mto_v30d","cl53mto_v60d","cl54mto_v90d","cl55mto_v120d","cl56mto_v180d",
"cl57mto_v1a","cl58mto_vm1a","cl59nom_grp_econom","cl60fec_ppago_cap","cl61per_pago_cap","cl62fec_ppago_int",
"cl63per_pago_int","cl64dia_mora","cl65perfil_venc ","cl66mto_cuota_pagar","cl67met_cal_int","cl68int_diferidos",
"cl69fec_ult_pcap","cl70mto_ult_pcap","cl71fec_ult_pint","cl72mto_ult_pint","cl73num_cliente","clColumna1","clColumna2",
"cl76modalidad","clColumna4","cl78sexo","cl79moneda","clColumna6","cl81segmento","cl83actividad_especial","cl84categoria_cambio",
]



def grouper(iterable, n, fillvalue=""):
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)

def _rename_columns(dfs_input,usecolms,flag):
        colmsdfs=dfs_input.columns.values.tolist()
        dict_pattern = {colmsdfs[i]: usecolms[i] for i in range(len(usecolms))}
        for indexA,indexB in dict_pattern.items():
                name=colsNames[indexB]
                dfs_input.rename(columns={indexA:flag+name}, inplace=True)

        return dfs_input

def _rename_out_dfs(dfsout,fdrop):
        for col in dfsout.columns.values:
                dfsout.rename(columns={f"{col}": f"{col.replace(f'{fdrop}','')}"},inplace=True) 
        return dfsout
        

def _read_file_b(usecolms,fName):
        df_raw=pd.read_csv(f'./{pathTemp}/files/{fName}.txt', sep='|'
                                ,dtype='object'
                                ,encoding = "ISO-8859-1"
                                ,header=None
                                #,usecols=usecolms
                               # ,nrows=100000
                                 ,keep_default_na=False
                                )
        
        df_raw.fillna('-NA', inplace=True)

        return _rename_columns(df_raw,usecolms,f'{ftxt01}')
        
def _read_file_a(usecolms,fName):
        df_raw=pd.read_csv(f'./{pathTemp}/files/{fName}.txt', sep='|'
                              ,dtype='object'
                              ,encoding = "ISO-8859-1"
                              ,header=None
                               ,keep_default_na=False
                               #,usecols=usecolms
                             # ,nrows=100000
                              )
        df_raw.fillna('-NA', inplace=True)
        return _rename_columns(df_raw,usecolms,f'{ftxt02}')

def _create_query(cntColumnas):
        strQuery=""
        for txt in range(cntColumnas):
                
                if txt == cntColumnas-1:
                        strQuery+=f"cl['COL{txt}'] == 'yes'"
                else:
                        strQuery+=f" col['COL{txt}'] == 'yes' and "
        return strQuery


def _info(flag):
        date=datetime.now()
        print(f"+{'-'*100}+") 
        print(f"|      \033[0;32;40m{date.strftime('%Y-%m-%d %H:%M:%S')} - [INFO] - {flag} \033[0m")
        print(f"+{'-'*100}+") 
        
def _print_en_columnas(lista, numfilas, ancho=25):
        for fila in zip(*grouper(lista, numfilas)):
                print("".join(f"{nombre:{ancho}s}" for nombre in fila))
    

def truncate(number, digits) -> float:
        nbDecimals = len(str(number).split('.')[1]) 
        if nbDecimals <= digits:
                return number
        stepper = 10.0 ** digits
        return math.trunc(stepper * number) / stepper
        
        
def rows_duplicated(dfRaw, colFilter):
        dfDuplicate = dfRaw[dfRaw.duplicated(colFilter)]
        rows=dfDuplicate.shape[0]
        if rows !=0:
                dfDuplicate.to_csv(f"./{pathTemp}/out/{fType}_rows_duplicados.txt",sep='~')
        return rows


if __name__ == '__main__':
        _ = system('cls')
       # DEFNIR COLUMNAS A COMPARAR Y COLUMNA MASTER RECORDAR QUE SE INICIA CONTANDO DESDE CERO
       # eqColumns=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14] analisis para tdc masterdata--lz
        
       # eqColumns=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,27]
             
        eqColumns=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
                   ,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42
                   ,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62
                   ,63,64,65,66,67,68,69,70,71]
        
      
        # master key column AT03
        keyPre=13
        
        eqacant=len(eqColumns)


        _info('Lectura de insumos txt a comparar')
      
        #############################################   LEER TXT  ######################################################
        
        ## lectura de archivos txt a comparar
        dfs_blka=_read_file_a(eqColumns,nFilea)
        dfs_blkb=_read_file_b(eqColumns,nFileb)
        
        #print(dfs_blkb.columns)
        
        # dfs_blka_e= dfs_blka[dfs_blka["wc_banco"].str.contains("E", case=True)]
        # dfs_blkb_e= dfs_blkb[dfs_blkb["cb_banco"].str.contains("E", case=True)]
        
         
        # obtner nombre de campos llave de ambos archivos 
        kpre_blka,kpre_blkb=dfs_blka.columns[keyPre],dfs_blkb.columns[keyPre]
              
        
        ## conteo de filas unicas
        print(f"\tCantidad de rows txt BLK_A - {nFilea}.txt :{dfs_blka.shape[0]:>7}")
        print(f"\tCantidad de rows txt BLK_B - {nFileb}.txt :{dfs_blkb.shape[0]:>7}")

        ## fill numero de prestamo en archivos
        dfs_blka[kpre_blka]=dfs_blka[kpre_blka].apply(lambda row: row.zfill(19))
        dfs_blkb[kpre_blkb]=dfs_blkb[kpre_blkb].apply(lambda row: row.zfill(19))
       
       
        ## filas duplicados en archivos txt a comparar
        print("\n\tFilas duplicadas en txt BLK_A :", rows_duplicated(dfs_blka,kpre_blka))
        print("\tFilas duplicadas en txt BLK_B :", rows_duplicated(dfs_blkb,kpre_blkb))
        
        # print("\n\tFilas duplicadas en txt BLK_A :", dfs_blka_a.shape[0])
        # print("\tFilas duplicadas en txt BLK_B :", dfs_blkb_e.shape[0])    
            

        
        ######################################## RESULTADO DE MERGE DF´s ######################################################    
        _info("Merge de archivos txt") 
        # inner join de df's por llaves unicas 
        df_result=dfs_blkb.merge(dfs_blka[dfs_blka.columns],left_on=kpre_blkb,right_on=kpre_blka)
        
        ## join para hacerla con 2 llaves la comparacion
        #df_result=dfs_blkb.merge(dfs_blka[dfs_blka.columns],left_on=[kpre_blkb,kruc_blkb],right_on=[kpre_blka,kruc_blka])
         
       
        print(f"\tRows input txt BLK_A :{dfs_blka.shape[0]:>7}")
        print(f"\tRows input txt BANCO :{dfs_blkb.shape[0]:>7}")  
        print(f"\tRows out txt MERGE   :{df_result.shape[0]:>7}") 
        print(f"\tRows out txt DIFFE   :{abs(df_result.shape[0] - dfs_blkb.shape[0]):>7}")    
   
                 
        print(df_result.sample(5).T)
        
        
               
        ######################################### COMPARANDO COL x COL  ####################################################
        _info("Analizando fxf & cxc")
        listcolResult=df_result.columns.values
        sizeResult=df_result.shape[0]
        ifisrt=0
        for inext in range(eqacant,len(listcolResult)):
                df_result[f"fout_{listcolResult[ifisrt].replace(f'{ftxt01}','')}"]=df_result.apply(lambda col: 'yes'
                                                        if col[listcolResult[ifisrt]].split() == col[listcolResult[inext]].split()
                                                        else 'no', axis=1)
                ifisrt +=1

         
        # selecionar solo columas con flag fout_
        nColflag= [col for col in df_result.columns.values if 'fout_' in col]  
        dfxfCOL= df_result[nColflag]
      
      
        # comparando flags en cada columna
        df_result['stats']=dfxfCOL.apply(lambda col: 1 if len(set(col.tolist()))==1 else 0 , axis=1)
        
        
        # procentaje de coincidencia por columnas 
        dictColumas={}
        for col in dfxfCOL.columns:
                nameCol=col.replace('fout_','')
                
                dictColumas[f'{nameCol}']=[np.sum(dfxfCOL[col]=='yes')
                                           ,np.sum(dfxfCOL[col]=='no')
                                           ,str(truncate(np.sum(dfxfCOL[col]=='yes')*100/sizeResult,2))+'%']
                        
        dfPorcentaje=pd.DataFrame.from_dict(dictColumas,orient='index')
        dfPorcentaje=dfPorcentaje.reset_index().rename(columns={'index':'Items',0:'Match',1:'No_Match',2:'Porcentaje'})
        dfPorcentaje.to_excel(f'./{pathTemp}/out/{fType}_coincidencia_por_columna.xlsx', index=False)
       
        # ######################################### ESTADISTICAS ########################################################    
        rTrue= np.sum(df_result['stats']==1)
        print(f"+{'-'*80}+") 
        print(f"\tCoincidencia para {rTrue} de {df_result.shape[0]} filas | {((rTrue*100)/df_result.shape[0]):.2f}%")      
        
        
        ############################### ANALISIS DE CUENTA QUE GENERAN ERROR ###########################################
        errorRows= df_result[(df_result['stats']==0) ]
        print("\tFilas con errores:  ",errorRows.shape[0])
        print(f"+{'-'*80}+")
               
        if errorRows.shape[0]>2:
                df_out_blkb=errorRows[[col for col in errorRows.columns.values if f'{ftxt01}' in col] ]  
                df_out_blka=errorRows[[col for col in errorRows.columns.values if f'{ftxt02}' in col] ]      
                dfxfCOL=errorRows[[col for col in errorRows.columns.values if 'fout_' in col] ] 
                df_out_blkb=_rename_out_dfs(df_out_blkb,f'{ftxt01}')
                df_out_blka=_rename_out_dfs(df_out_blka,f'{ftxt02}')
                dfxfCOL=_rename_out_dfs(dfxfCOL,'fout_')
                
                
                df_out_blkb['FTXT']='BLK_B'
                dfxfCOL['FTXT']='FLAG'
                df_out_blka['FTXT']='BLK_A'
                
                _info('Mostrando data con error')
                dfout_Final= pd.concat([df_out_blkb.T,df_out_blka.T,dfxfCOL.T], axis=1)
                print(errorRows.sample(5).T)
                dfout_Final=dfout_Final.T
                  

                _info('Salvando data con error')
                dfout_Final.to_csv(f'./{pathTemp}/out/{fType}_filas_con_error.txt',sep='|')
                print("Data guardada correctamente...")
                
         # show df's not macht
       # _print_en_columnas(errorRows[f'{kpre_blkb}'].tolist(),10)
        
        #################################  ANALISIS DE CUENTA EN ESTADO OK #############################################
        # _info('Mostrando data ok')
        # goodRows= df_result[(df_result['stats']==1) ]
        # if goodRows.shape[0]>2:
                
        #         print(goodRows.sample(3).T)      
        
        _info('Proceso terminado')