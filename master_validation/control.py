import pandas as pd

import json
import re
import os
import datetime
import time

from .query import Query

class Control():
    """
    Clase del maestro de validaciones. Se encarga de construir y ejecutar los controles
    definidos en el dataframe de controles, así como de obtener los detalles para los 
    controles que no pasen.
    """ 
    
    def __init__(self,ruta_config="",df_input=pd.DataFrame(),logger=object,scrt=object, anli=object, variacion = False, ver_filtro = False, **kwargs) :
        """
        Args:
            self
            ruta_p (str, optional): Ruta completa (Inactivo). Defaults to "".
            df_input (pandas.DataFrame, optional): Dataframe de controles filtrado por el pilar a ejecutar. Defaults to pd.DataFrame()
            logger (object, optional): Instancia del logger de sparky. Defaults to object
            variacion (bool, optional): Activa el flujo de comprobación de variacion de controles. Defaults to False.
            ver_filtro (bool, optional): Activa la construcción de la columna filtro_aplicado. Defaults to False.
        """
        #super().__init__(**kwargs)
        self.hp = kwargs.get("helper",None)
        self.ruta_config=ruta_config
        self.zona_tabla_insumo = ""
        self.pilares = ""
        self.tabla=""
        self.columna = ""
        self.log = logger
        self.scrt = scrt
        self.variacion = variacion
        self.ver_filtro = ver_filtro
        if len(df_input)>0:
            self.df_insumo=df_input
        else:
            self.df_insumo=pd.DataFrame()
        self.anli = anli
    
    
    def construir_controles(self,df=pd.DataFrame(),n=0):
        """
        Función que se encarga de obtener los datos y construir las columnas 
        necesarias para la ejecución de los controles
        Args:
            self
            df (pandas.DataFrame, optional): Dataframe con la información de los controles. Defaults to pd.Dataframe()
            n (int, optional): Indicador del pilar que se ejecuta. Defaults to 0
        Returns:
            estado (Boolean): True o False
            df_p (pandas.DataFrame): Dataframe de salida con la información de controles construida
        """
        estado=False
        df=df.copy()
        df_p = pd.DataFrame()
        df_p = self._add_ruta_sql(df,n)
        if len(df_p) > 0:
            estado=True
            return estado,df_p
        return estado,df_p
        
    
    def filtrar_controles(self,df_insumo=pd.DataFrame()):
        """
        Función que se encarga de filtrar los controles que se ejecutarán según el pilar
        Args:
            self
            df_insumo (pandas.DataFrame, optional): Dataframe con la información de los controles. Defaults to pd.Dataframe()
        Returns:
            df_filtrado (pandas.DataFrame): Dataframe con la información de los controles filtrados
        """
        df_filtrado = df_insumo.copy()
        if len(df_filtrado) > 0:
            df_ree = df_filtrado.loc[(df_filtrado['n_ejecucion'] > 1) & (df_filtrado["salida"] == 1)]
            if len(df_ree) > 0:
                df_filtrado = df_ree
        return df_filtrado
    
    def ejecutar_controles(self,df_insumo=pd.DataFrame()):
        """
        Función que se encarga de la ejecución de los controles. Contiene la lógica para
        ejecutar controles estándar y avanzados, así como obtener los detalles de los controles
        que no pasen. Adicionalmente, si la variacion está activada, calcula la variación del 
        control que su ejecución inmediatamente anterior
        Args:
            self
            df_insumo (pandas.DataFrame, optional): Dataframe con la información de los controles. Defaults to pd.Dataframe()
            n_ejecucion (int, optional): Número de ejecuciones del pilar (Inactivo). Defaults to 1
            n (int, optional): Indicador del pilar que se ejecuta. Defaults to 0
        Returns:
            estado (bool): True o False
            df_insumo (pandas.DataFrame): Dataframe con la información de los controles ya ejecutados
        """
        df_filtrado= self.filtrar_controles(df_insumo)
        if self.scrt.validar_query(df_filtrado):
            if self.variacion:
                df_filtrado = self.anli.gestionar_variacion(self, df = df_filtrado)
            for index,row in df_filtrado.iterrows():
                try:
                    df= self.hp.obtener_dataframe(row["query"])
                    if row['tipo_ctrl'] == 'ESTANDAR':
                        df_insumo.at[index, "salida"] = df.iloc[0, 0]
                    elif row['tipo_ctrl'] == 'AVANZADO':
                        df_insumo.at[index, "salida"] = self.control_avanzado(row,df)
                    df_insumo.at[index, "n_ejecucion"] = int(df_insumo.at[index, "n_ejecucion"])+1

                    if df_insumo.at[index, "salida"] == 0:
                        df_insumo.at[index, "estado"] = "OK"
                    elif df_insumo.at[index, "salida"] == 1:
                        df_insumo.at[index, "estado"] = df_insumo.at[index,"caso_falla"]
                    
                    if self.variacion and row['variacion'] == 1 or df_insumo.at[index, "salida"] == 1:
                        row_detalle = self._consulta_auxiliar(row)
                        df_valor_aux= self.hp.obtener_dataframe(row_detalle["query"])
                        valor_aux = df_valor_aux.iloc[0,0] if df_valor_aux.iloc[0,0] else 0

                        if self.variacion and row['variacion'] == 1:
                            df_insumo.at[index, "result_ctrl"] = valor_aux
                            df_insumo.at[index, "estado_variacion"], df_insumo.at[index, "salida"] = self.anli.calcular_variacion(row, valor_aux)


                    if df_insumo.at[index, "estado"] != "OK":
                        text_detalle = self.get_detalle(row)
                        self.log.print_fallo_control(row['ctrl'],
                                                     row['nivel'],
                                                     row['tabla'],
                                                     text_detalle.format(valor_aux),
                                                     row_detalle["query"])
                        time.sleep(3)
                except Exception as e:
                    self.log.print_error(e)
                    raise self.log.list_error('control',1, index)
            return True,df_insumo
        return False, df_insumo

        
    def _consulta_auxiliar(self, row, d=1):

        '''
        Función que obtiene el detalle para los controles que no pasen o que deban pasar por analisis de variación.
        Genera una nueva consulta en la LZ según el tipo de control para obtener información númerica que detalla
        el suceso.

        Args:
            self
            row (pandas.Series): Fila del df que en la que se encuentra el control a analizar
        Returns:
            row_aux (pandas.Series): Fila del df con la información actualizada
        '''
        
        row_aux = row.copy()
        
        data_sql_aux = self.qry.get_query(row_aux["ctrl"],d)
        row_aux["ruta_sql"]=data_sql_aux["ruta_sql"]
        row_aux["num_params"]=str(data_sql_aux["params"])

        _,self.registros_aux = self._obtener_json()
        registro_aux = self._obtener_registro(row_aux["nivel"],row_aux["ctrl"],row_aux["parametro_ctrl"],1)
        row_aux['registro'] = registro_aux

        dato_aux= str(self.qry.get_params(tipo=2,registro=registro_aux))
        row_aux['params_reg']=self._limpieza(dato_aux)

        df_aux = pd.DataFrame(row_aux).T
        df_aux.reset_index(drop=True, inplace=True)
        params,query,_,_= self.qry.get_construir_query(df_aux,0,d)
        row_aux["params_gral"]=str(params)
        row_aux["query"]=str(query)
        
        return row_aux
    
    def get_detalle(self,row):

        '''
        Función que obtiene la plantilla del mensaje de detalle de los controles
        que no pasaron

        Args:
            self
            row (pandas.Series): Fila del df que en la que se encuentra el control que no paso
        Returns:
            detalle (str): Plantilla del mensaje del control
        '''

        estado_json,self.detalle = self._obtener_json()
        detalle = self._obtener_registro(row["nivel"],row["ctrl"],row["parametro_ctrl"],d=2)
        
        if estado_json:
            return detalle
            
    
    def _add_ruta_sql(self,df_plan=pd.DataFrame(),n=0):
        """
        Función privada que se encarga de instanciar la clase Query y construir, según el tipo
        de control, las columnas necesarias para su ejecución
        Args:
            self
            df_plan (pandas.DataFrame, optional): Dataframe con la información de los controles a ejecutar. Defaults to pd.DataFrame()
            n (int, optional): Indicativo del pilar en ejecución (Inactivo): Defaults to 0
        Returns:
            df_plan (pandas.DataFrame): Dataframe con la información de los controles a ejecutar
        """
        self.qry = Query(logger=self.log, ruta_config=self.ruta_config)
        df_plan =df_plan.copy()
        estado_json,self.registros = self._obtener_json()
        params={}

        for index, row in df_plan.iterrows():
            if row['tipo_ctrl'] == 'ESTANDAR':
                param_ctrl = row['parametro_ctrl'].split(':')[0]
                if row['ctrl'] == 'range':
                    if row['nivel'] not in self.registros['registros'] or row['ctrl'] not in self.registros['registros'][row['nivel']]:
                        raise self.log.list_error('control', 0, '{0}-{1}-{2}'.format(row['nivel'],row['ctrl'],param_ctrl))
                    continue
                else:
                    if row['nivel'] not in self.registros['registros'] or row['ctrl'] not in self.registros['registros'][row['nivel']] or param_ctrl not in self.registros['registros'][row['nivel']][row['ctrl']]:
                        raise self.log.list_error('control', 0,'{0}-{1}-{2}'.format(row['nivel'],row['ctrl'],param_ctrl))
                    continue

        if estado_json:
            if len(df_plan)>0:
                for  index,row in df_plan.iterrows():
                    if row['tipo_ctrl'] == 'ESTANDAR':
                        data_sql = self.qry.get_query(row["ctrl"])
                        df_plan.loc[index,"ruta_sql"]=data_sql["ruta_sql"]
                        df_plan.loc[index,"num_params"]=str(data_sql["params"])
                        registro = self._obtener_registro(row["nivel"],row["ctrl"],row["parametro_ctrl"])
                        df_plan.loc[index,"registro"]=registro
                        dato= str(self.qry.get_params(tipo=2,registro=registro))
                        df_plan.loc[index,"params_reg"]=self._limpieza(dato)
                        params,query,estado,filtro= self.qry.get_construir_query(df_plan,index)
                        if estado :
                            if self.ver_filtro:
                                df_plan.loc[index,"filtro_aplicado"]=str(filtro)
                            df_plan.loc[index,"params_gral"]=str(params)
                            df_plan.loc[index,"query"]=str(query)
                        else:
                            return df_plan
                    elif row['tipo_ctrl'] == 'AVANZADO':
                        if row['ctrl'] == 'tablas_p':
                            df_plan.loc[index,"query"]='SHOW FILES IN {0}'.format(row['tabla'])
                        elif row['ctrl'] == 'recursos':
                            route_params = row['parametro_ctrl'].replace(',','/')
                            sql_ruta = self.qry._get_sql_archive(route_params=route_params)

                            if sql_ruta == '':
                                raise self.log.list_error('control',4,index)
                            else:
                                with open(sql_ruta) as in_file_sql:
                                    sql_query_full = in_file_sql.read()
                                indice_query = [query for query in sql_query_full.split(';') if query != '']
                                df_plan.loc[index,"query"]='EXPLAIN {0}'.format(indice_query[0])
            else:
                self.log.print_error("El pilar se encuentra vacio al momento de filtrar por el mismo")
                return df_plan
            return df_plan
        return df_plan    
    
    
    def _limpieza(self,texto=""):
        """
        Función  privada para limpiar strings de los parametros de los controles
        Args:
            self
            texto (str, optional): Texto a limpiar. Defaults to ""
        Returns:
            lim_texto (str): Texto limpiado
        """
        lim_texto=texto.replace("'n_col',","").replace(",'n_col'","")
        lim_texto=lim_texto.replace("[","").replace("]","")
        lim_texto=lim_texto.replace("'","").replace(" ","")
        return lim_texto
    
    def _obtener_registro(self,nivel="",nom_control="",ctrl_param="", d=0):
        """
        Función  privada que se encarga de obtener el valor de registro especifico de cada control
        dependiendo de la información contenida en el dataframe. Puede obtener registros para
        la construcción del query, del query de detalle o del mensaje de detalle.
        Args:
            self
            nivel (str, optional): Nivel del control al cual se le obtendrá el registro. Defaults to ""
            nom_control (str, optional): Nombre del control al cual se le obtendrá el registro. Defaults to ""
            ctrl_param (str, optional): Parametros del control al cual se le obtendrá el registro. Defaults to ""
            d (int, optional): Indicativo del tipo de registro a obtener. Defaults to 0
        Returns:
            registro (str): String del registro obtenido.
        """
        registro=""
        if pd.isna(ctrl_param) or ctrl_param=="":
            ctrl_param=""
        if pd.isna(nom_control) or nom_control=="":
            nom_control=""
        json_registro = self.registros

        registro_valor = {
            0:"registros",
            1:"registros_detalle",
            2:"detalle"
        }

        for (registro_key,valor_r) in json_registro.items():
            if registro_key.lower() == registro_valor[d]:
                for (nivel_key,valor_n) in valor_r.items():
                    if nivel_key.lower() == nivel.lower():
                        for (ctrl_key,valor_c) in valor_n.items():
                            if ctrl_key.lower()==nom_control.lower():
                                if nom_control == 'reg':
                                    argumento = ctrl_param.split(":",1) #0 arg 1 valor
                                    if argumento[0].lower() in valor_c.keys():#si no esta el argumento error
                                        registro = valor_c[argumento[0].lower()]
                                        break
                                else:
                                    if ":" in str(ctrl_param): 
                                        argumento = ctrl_param.split(":") #0 arg 1 valor
                                        if argumento[0].lower() in valor_c.keys():#si no esta el argumento error
                                            registro = valor_c[argumento[0].lower()]
                                            break
                                    elif "|" in str(ctrl_param): 
                                        registro = valor_c[nom_control.lower()] #0 arg 1 valor
                                        break
                                    elif "" == str(ctrl_param):
                                        registro=valor_c[ctrl_key]
                                        break
                    else:
                        continue
                    break
            else:
                continue
            break
        if registro=="":
            registro ="0"              
            registro ="0"             
        return registro
        
    
    def _obtener_json(self):
        """
        Función privada para obtener el json de registros

        Args:
            self
        Returns:
            estado (boolean): True o False
            archivos (dict): Diccionario con la información del json de registros
        """
        estado=False
        ruta = self.qry.ruta
        
        with open(f'{os.path.abspath(ruta)}/registro/registros.json') as infile:
            archivos = json.load(infile)

        estado=True
        return estado,archivos
    
    def control_avanzado(self,row,df):
        """
        Función que tiene la lógica para verificar los controles avanzados de consumo
        de recursos y tablas pequeñas. De acuerdo con la información obtenida del query ejecutado
        la función se encarga de definir si el control pasó o no así como de proporcionar detalles
        en caso de que no pase.
        Args:
            self
            row (pandas.Series): Fila del dataframe de controles que contiene el control avanzado ejecutado
            df (pandas.DataFrame): Dataframe con información obtenida de la query ejecutada
        Returns:
            (int): 0 o 1, indicativo de que el control pasó o no pasó, respectivamente
        """
        if row['ctrl'] == 'tablas_p':
            df['Size'] = df['Size'].apply(self.remover_sufijos_peso)
            if (df['Partition'].isna().any()) or (any(df['Partition']=='')):
                #Sin particiones
                self.log.print_warning(" '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' ")
                self.log.print_warning("El control tablas_p no paso.")
                self.log.print_warning('La tabla no tiene particiones relacionadas a uno o más archivos')
                self.log.print_warning(" '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' ")
                return 1
            else:
                #Con particiones
                #Crea un nuevo dataframe filtrado
                df_files_p = df[df['Size'] <= 128].groupby('Partition').size().reset_index(name='files')
                if df_files_p['files'].gt(1).any():
                    conteo = df_files_p['files'].gt(1).count()
                    self.log.print_warning(" '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' ")
                    self.log.print_warning("El control tablas_p no paso.")
                    self.log.print_warning('{0} particiones contienen archivos pequeños, la más reciente de este grupo es {1}'.format(conteo,df_files_p['Partition'].iat[-1]))
                    self.log.print_warning(" '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' ")
                    return 1
                else:
                    return 0
        elif row['ctrl'] == 'recursos':
            estado = True
            for _,row in df.iterrows():
                if row['Explain String'].startswith('Per-Host Resource Estimates:'):
                    size_resource_num = self.remover_sufijos_peso(row['Explain String'].split('=')[1])
                    if size_resource_num > 20000:
                        self.log.print_warning(" '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' ")
                        self.log.print_warning("El control recursos no paso.")
                        self.log.print_warning('La memoria estimada de la consulta supera el maximo de 20GB')
                        self.log.print_warning(" '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' ")
                        return 1
                    else:
                        return 0
        
    def remover_sufijos_peso(self,value):
        """
        Función remover los sufijos de peso obtenidos al ejecutar el control
        avanzado de consumo de recursos
        Args:
            self
            value (str): Valor obtenido del consumo de recursos de una consulta
        Returns:
            numeric_value: Valor obtenido del consumo de recursos de una consulta convertido en MB
        """
        if 'MB' in value:
            numeric_value = float(re.sub(r'[^0-9.]', '', value))
        elif 'KB' in value:
            numeric_value = float(re.sub(r'[^0-9.]', '', value))/1000
        elif 'GB' in value:
            numeric_value = float(re.sub(r'[^0-9.]', '', value))*1000
        else:
            numeric_value = 0
        return numeric_value