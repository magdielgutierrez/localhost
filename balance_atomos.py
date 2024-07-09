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
ftxt01, nFilea = 'rcob', 'AT03_20240131'
ftxt02, nFileb = 'rbnk_', 'bk_prstamo_ene_2024'

pathTemp = '2024-01-Ene'
fType = 'pre_ene_'



# cOLUMNAS DE AT03
colsNames=["cl_01fecha","cl_02codigo_del_banco","cl_03subsidiaria","cl_04preferencial_no_preferencial"
           ,"cl_05aplica_feci","cl_06tipo_credito","cl_07facilidad_crediticia","cl_08clasificacion_prestamo"
           ,"cl_09destino","cl_10codigo_region","cl_11id_cliente","cl_12tamaño_empresa","cl_13genero"
           ,"cl_14numero_del_prestamo","cl_15nombre_cliente","cl_16id_grupo_economico","cl_17tipo_relacion"
           ,"cl_18tipo_actividad","cl_19tasa_interes","cl_20valor_inicial","cl_21intereses_por_cobrar"
           ,"cl_22fecha_inicial_prestamo","cl_23fecha_vencimiento","cl_24fecha_refinanciamiento"
           ,"cl_25fecha_restructuracion.","cl_26numero_de_facilidad_anterior"
           ,"cl_27primera_garantia","cl_28valor_primera_garantia","cl_29segunda_garantia"
           ,"cl_30valor_segunda_garantia","cl_31tercera_garantia","cl_32valor_tercera_garantia"
           ,"cl_33cuarta_garantia","cl_34valor_cuarta_garantia","cl_35quinta_garantia","cl_36valor_quinta_garantia"
           ,"cl_37provision","cl_38provision_niif","cl_39provision_complementaria_no_niif","cl_40saldo"
           ,"cl_41cant_cuotas","cl_42mto_xv30d","cl_43mto_xv60d","cl_44mto_xv90d","cl_45mto_xv120d"
           ,"cl_46mto_xv180d","cl_47mto_xv1a","cl_48mto_xv1a5a","cl_49mto_xv5a10a","cl_50mto_xvm10a"
           ,"cl_51cant_cuotas","cl_52mto_v30d","cl_53mto_v60d","cl_54mto_v90d","cl_55mto_v120d","cl_56mto_v180d"
           ,"cl_57mto_v1a","cl_58mto_vm1a","cl_59nombre_grupo_economico","cl_60fecha_de_proximo_pago_capital"
           ,"cl_61periodicidad_de_pago_de_capital","cl_62fecha_de_proximo_pago_interes","cl_63periodicidad_de_pago_de_intereses"
           ,"cl_64dias_de_mora","cl_65perfil_de_vencimiento","cl_66monto_de_la_cuota_a_pagar","cl_67metodologia_de_cálculo_de_intereses"
           ,"cl_68intereses_diferidos","cl_69fecha_de_ultimo_pago_capital","cl_70monto_del_ultimo_pago_capital"
           ,"cl_71fecha_de_ultimo_pago_interes","cl_72monto_del_ultimo_pago_interes","cl_73categoria_cambio"]


# colsNames=["Fecha","Codigo_Banco","Numero_Prestamo","Numero_Ruc_Garantia","Id_Fideicomiso","Nombre_Fiduciaria"
#       ,"Origen_Garantia","Tipo_Garantia","Tipo_Facilidad","Id_Documento","Nombre_Organismo","Valor_Inicial"
#       ,"Valor_Garantia ","Valor_Ponderado","Tipo_Instrumento","Calificacion_Emisor","Calificacion_Emisision"
#       ,"Pais_Emision","Fecha_Ultima_Actualizacion","Fecha_Vencimiento","Tipo_Poliza","Codigo_Region"
#       ,"clave_pais",'clave_empresa', 'clave_tipo_garantia', 'clave_subtipo_garantia', 'clave_tipo_pren_hipo', 'numero_garantia']     


# colsNames=['banco' ,'fecha_desembolso'  ,'fecha_vencimiento',
# 'monto_aprobado' ,'saldo','tasa' ,'org' ,'logo' ,                
# 'dias_mora','valor_cuota','aplica_feci' ,'fecha_ult_pago_cap',
# 'valor_ult_pago_cap','fecha_ult_pago_int','valor_ult_pago_int']


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
        df_raw=pd.read_csv(f'./{pathTemp}/files/{fName}.txt', sep='~'
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
                   ,63,64,65,66,67,68,69,70,71,72]
        
        # eqColumns=[0,1,2,3,4,5,8,9,10,12,13,14,17,18,19,20,21,22,23,39,40,41,42,43,
        #              44,45,46,47,48,49,50,51,52,53,54,55,56,57,60,62,63,65,66,67,68,69,70,71]
          
        # master key column AT03
        keyPre=13
        keyRuc=10
        
        # ## master key fro AT12 
        # keyPre=2
        # keyRuc=22
        
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
        kruc_blka,kruc_blkb=dfs_blka.columns[keyRuc],dfs_blkb.columns[keyRuc]
        
        
        ## conteo de filas unicas
        print(f"\tCantidad de rows txt BLK_A - {nFilea}.txt :{dfs_blka.shape[0]:>7}")
        print(f"\tCantidad de rows txt BLK_B - {nFileb}.txt :{dfs_blkb.shape[0]:>7}")

        ## fill numero de prestamo en archivos
        dfs_blka[kpre_blka]=dfs_blka[kpre_blka].apply(lambda row: row.zfill(10))
        dfs_blkb[kpre_blkb]=dfs_blkb[kpre_blkb].apply(lambda row: row.zfill(10))
       
       
        ## filas duplicados en archivos txt a comparar
        print("\n\tFilas duplicadas en txt BLK_A :", rows_duplicated(dfs_blka,kpre_blka))
        print("\tFilas duplicadas en txt BLK_B :", rows_duplicated(dfs_blkb,kpre_blkb))
        
        # print("\n\tFilas duplicadas en txt BLK_A :", dfs_blka_a.shape[0])
        # print("\tFilas duplicadas en txt BLK_B :", dfs_blkb_e.shape[0])
        
        
        ## fill para unificar cedula para AT03
        # dfs_blka[kruc_blka]=dfs_blka[kruc_blka].apply(lambda row: row.replace('-','').zfill(15))
        # dfs_blkb[kruc_blkb]=dfs_blkb[kruc_blkb].apply(lambda row: row.replace('-','').zfill(15))
       
        dfs_blka[kruc_blka]=dfs_blka[kruc_blka].apply(lambda row: re.sub(r'[A-Z\-]', '', row).zfill(15))
        dfs_blkb[kruc_blkb]=dfs_blkb[kruc_blkb].apply(lambda row: re.sub(r'[A-Z\-]', '', row).zfill(15))
      
      
        ## fill para numero de  garantia  para AT12
        dfs_blka[kruc_blka]=dfs_blka[kruc_blka].apply(lambda row: row.zfill(15))
        dfs_blkb[kruc_blkb]=dfs_blkb[kruc_blkb].apply(lambda row: row.zfill(15))
        

        ########################################### DUPLICADOS EN  DF´s #######################################################  
#         eliminar duplicados en atomo
#        dfs_blka = dfs_blka.drop_duplicates(subset=['wc_14numero_del_prestamo'])
#        dfs_blka['flag']='BLK_A'
            
        
        # n = len(pd.unique(dfs_blka['wc_14numero_del_prestamo']))
        # print("\tNo.of.unique values cobis :",  n)
   
        # print(dfs_blkb[kruc_blkb].head(5))
#         # print(dfs_blka[kruc_blka].head(5))

    
        
        
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
        ######################################### OBTENER DIFERENCIAS ################################################
        #key_match_list= list(df_result.bk_14numero_del_prestamo.values.tolist())
        # print(key_match_list)
        # print(dfs_blka['wc_numero_garantia'])
#         NOT_FOUND_IN_BLK_A=dfs_blka [~dfs_blka['wc_numero_garantia'].isin(key_match_list)]
#       # print(NOT_FOUND_IN_BLK_A.shape[0]) 
#         NOT_FOUND_IN_BLK_A.to_csv(f"./{pathTemp}/out/{fType}_no_match_en_atomo_banco.txt",sep='~')
        
        # NOT_FOUND_IN_BANCO=dfs_blkb [~dfs_blkb['wk_14numero_del_prestamo'].isin(key_match_list)]
        # NOT_FOUND_IN_BANCO[kpre_blkb].to_csv(f"./{pathTemp}/out/{fType}_existe_banco_no_cobis.txt",sep='~', index=False)
        # # print(NOT_FOUND_IN_BANCO.shape[0]) 
           
        
               
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