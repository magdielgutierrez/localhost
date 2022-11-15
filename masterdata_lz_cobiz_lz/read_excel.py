import pandas as pd 

dfs_tbln= pd.read_excel('./Diccionario_de_Datos_y_Mapping_Masterdata.xlsx',
                        skiprows=2, sheet_name='Campos por Tabla y Mapping V+',
                        usecols=range(1,10))

filtertbl='vpscuenta'
dfs_tbln=dfs_tbln.loc[dfs_tbln['Tabla'] == filtertbl]

dfs_tbln.to_excel(f'{filtertbl}.xlsx', index=False)