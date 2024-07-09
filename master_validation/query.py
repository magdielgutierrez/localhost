import pkg_resources
import os
import re
import pandas as pd
from datetime import date, timedelta
import json
# from master_validation.control import Control
class Query():
    """
    Clase del maestro de validaciones. Se encarga de entregar la ruta 
    y los parámetros de funcionamiento para un control solicitado.
    """
    
    def __init__(self ,logger=object,ruta_config="",**kwargs): 
        """
        Args:
            self
            logger (object, optional): Instancia del logger de sparky. Defaults to object
        """
        self.ruta = self._get_sql_route()
        self.params = {"name_control": "","ruta_sql":"","params":[],"ozie":""}
        self.df_plan= pd.DataFrame()
        self.log = logger
        self.ruta_config=ruta_config
    
    def _get_sql_route(self):
        """
        Función Privada que arroja la ruta a la carpeta static.
        Returns:
            ruta (str): ruta de la carpeta static.
        """
        return pkg_resources.resource_filename(__name__, 'static')
    
    def _get_sql_archive(self,route_params = None):
        """
        Función privada que entrega la ruta al control exacto que se 
        solicita al invocar la clase. La función ingresa a la carpeta 
        de controles desde la dirección entregada por get_sql_route, 
        repasa todos los archivosy busca si hay alguno que coincida con 
        el nombre del control solicitado; en tal caso, el parámetro "Ruta_sql" 
        se actualiza a la ruta hasta este control. En el caso de ser usada para
        el control de consumo de recursos, la función obtiene la ruta al archivo sql
        especificado por el usuario

        Args:
            self
            route_params (list, optional): Listado con los parametros de la ruta (especial para el control de recursos). Defaults to None
        """
        ruta_control=""

        if route_params is None:
            for root, _, files in os.walk("{}/control/".format(self.ruta)):
                for file in files:
                    if file.startswith(self.params["name_control"]) and file.endswith(".sql"):
                        ruta_control = os.path.join(root, file)
                        break
            if ruta_control == "":
                raise self.log.list_error('query',0,self.params["name_control"])
            else:
                self.params["ruta_sql"]=ruta_control
        else: 
            ruta_completa = os.path.join(self._split_route(), route_params)

            if os.path.isfile(ruta_completa):
                return ruta_completa
            else:
                self.log.print_error("La ruta o el archivo no existen.")
    
    def _split_route(self):
        """
        Función privada que parte la ruta de donde se encuentre instalado el maestro
        con el objetivo de encontrar la ruta del directorio base del proyecto, y
        así poder obtener la ruta al archivo sql especificado en el control
        de consumo de recursos

        Args:
            self
        Returns:
            ruta (str): Ruta del directorio base del proyecto
        """
        if 'venv' in self.ruta:
            left_part, _ = self.ruta.split('venv', 1)
            return left_part.rstrip(os.path.sep)
        else:
            return self.ruta
    
    def get_params(self,tipo=1,registro="",d=0):
        """
        Función privada que busca las expresiones regulares de la forma {xxx}
        y las ingresa en la lista de parámetros del control solicitado. Puede obtener
        parametros tanto para los querys principales de los controles como para
        los querys de detalle

        Args:
            self
            tipo (int, optional): Especifica que ruta debe seguir la función para buscar e ingresar los parametros. Defaults to 1
            registro (str, optional): Registro del control solicitado en el que se buscará parametros en los tipos 2 y 3. Defaults to ""
            d (int, optional): Indicador del tipo de SQL que debe obtener (principal o detalle) para el control solicitado. Defaults to 0

        Returns:
            str_params(list): Lista de parametros del control solicitado
        """
        val_param = "{+[0-9a-zA-Z_]+}"
        str_params=[]
        if tipo==1:
            if self.params["ruta_sql"] != "":
                with open(self.params["ruta_sql"], encoding='utf-8') as in_file_sql:
                    sql_query_full = in_file_sql.read()
                    sql_query = sql_query_full.split(';')[d]
                    control_r =re.findall(val_param, sql_query)
                    for controls in control_r:
                        patron =str(controls).replace('{','').replace('}','')
                        if patron not in str_params:
                            str_params.append(patron) 
            if len(str_params)>0:
                return str_params        
        elif tipo ==2 :
            if registro!="":
                str_params=[]
                control_r =re.findall(val_param, registro)
                for controls in control_r:
                    patron =str(controls).replace('{','').replace('}','')
                    if patron not in str_params:
                        str_params.append(patron) 
                if len(str_params)>0:
                    return str_params
        elif tipo ==3 :
            if registro!="":
                str_params=[]
                control_r =re.findall(val_param, registro)
                for controls in control_r:
                    patron =controls
                    if patron not in str_params:
                        str_params.append(patron) 
                if len(str_params)>0:
                    return str_params
                #error: No existen parametros en el control para crear el query
            #Error: No hay control para realizar la construccion del query
        return str_params
    
    def get_query(self,name_control = "",d=0):
        """
        Función que se asegura de que exista la ruta hasta el control y 
        la lista de los parámetros para el query en caso afirmativo, 
        devuelve la lista de estos últimos.

        Args:
            self
            name_control (str, optional): Nombre del control. Defaults to ""
            d (int, optional): Indicador del tipo de SQL que debe obtener (principal o detalle) para el control. Defaults to 0
        Returns:
            self.params (list): los parámetros de entrada para el control seleccionado.
        """
        self.params = {"name_control": name_control,"ruta_sql":"","params":[]}
        try:
            self._get_sql_archive()
            self.params["params"]= self.get_params(1,d=d)
        except  ReferenceError as error_f:
            self.log.print_error(error_f) 
        return self.params
    
    def get_construir_query(self,df=pd.DataFrame(),indice=0,d=0):
        """
        Función que se encarga de construir el query del control solicitado
        según los parametros ya obtenidos

        Args:
            self
            df (pandas.DataFrame, optional): Dataframe con la información de los controles a ejecutar. Defaults to pd.DataFrame()
            indice (int, optional): Indice dentro de dataframe del control solicitado. Defaults to 0
            d (int, optional): Indicador del tipo query que debe construir (principal o detalle) para el control. Defaults to 0
        Returns:
            params (dict): Diccionario con los valores de las columnas necesarias para la ejecución del control
            query (str): Query construido para la ejecución del control
            estado (boolean): True o False
        """
        df = df
        indice=indice
        query=""
        sql_query=""
        params={}
        estado=False
        with open(df.at[indice,"ruta_sql"], encoding='utf-8') as in_file_sql:
            sql_query_full = in_file_sql.read()
            sql_query = sql_query_full.split(';')[d]

        if len(sql_query) > 0:
                #self.df_plan["params_reg"] = self.get_params(tipo=2,registro=row["registro"])
                params = self._contruir_params(params=params,df=df,indice=indice)
                try:
                    filtro = self.replace_params(params['filtro'],params)
                    query = self.replace_params(sql_query,params)
                    if query!='':
                        estado= True
                except Exception as e:
                    self.log.print_error("Error: No reemplazar en el query {} la el parametro de control {}".format(sql_query,e))
                    return params, query, estado, filtro
                return params, query, estado, filtro
        return  params, query, estado, filtro

    def _contruir_params(self,params=None,df=pd.DataFrame(),indice=""):
        """
        Función privada que se encarga de construir las columnas para la ejecución del 
        control solicitado

        Args:
            self
            df (pandas.DataFrame, optional): Dataframe con la información de los controles a ejecutar. Defaults to pd.DataFrame()
            indice (int, optional): Indice dentro de dataframe del control solicitado. Defaults to 0
        Returns:
            params (dict): Diccionario con los valores de las columnas necesarias para la ejecución del control
        """
        if params is None:
            params = {}
        df=df.copy()
        indice=indice
        params["zonatabla"]=self._construir_tabla(df,indice)
        params["ozie"]=df.at[indice,"tabla"].replace(".","_")
        params["nom_columna"]=df.at[indice,"nom_columna"]
        params["control"]= df.at[indice,"registro"]
        params["filtro"]=df.at[indice,"filtro_tabla"]
        params.update(self._contruir_filtro(df,indice))
        params.update(self._construir_control(df,indice))
        return params
    
    def _construir_tabla(self,df=pd.DataFrame(), indice = ""):
        """
        Función privada que extrae los parametros ingresados para la tabla
        para que sean empleados en la construcción del query

        Args:
            self
            df (pandas.DataFrame, optional): Dataframe con la información de los controles a ejecutar. Defaults to pd.DataFrame()
            indice (int, optional): Indice dentro de dataframe del control solicitado. Defaults to 0
        Returns:
            p_tabla (str): Valor de zonatabla a usar en el control
        """
        p_tabla = str(df.at[indice,"tabla"])
        params_tabla = self.get_params(tipo=2, registro=p_tabla)

        if params_tabla:
            params_tabla_config = self._definir_params_config(params_tabla)
            for param in params_tabla_config:
                p_tabla = p_tabla.replace("{"+f"{param}"+"}",str(params_tabla_config[param]))

        return p_tabla
    
    def _contruir_filtro(self, df=pd.DataFrame(), indice=""):
        """
        Función privada que obtiene los datos de filtro de los controles y los 
        transforma para que sean empleados en la construcción del query

        Args:
            self
            df (pandas.DataFrame, optional): Dataframe con la información de los controles a ejecutar. Defaults to pd.DataFrame()
            indice (int, optional): Índice dentro de dataframe del control solicitado. Defaults to 0
        Returns:
            fechas (dict): Diccionario con los valores del filtro para la ejecución del control
        """
        params_filtro = df.at[indice, "filtro_tabla"]
        dia_actual = date.today()
        dia_t = dia_actual
        fechas = {}
        try:
            t_n = str(df.at[indice, "t_n"])
            if t_n == "" or t_n == "0":
                t_n = 0
            elif int(t_n) > 0:
                t_n = int(t_n)
                dia_t = dia_actual - timedelta(int(t_n))
        except Exception as e:
            self.log.print_error("Error el valor de la columna filtro no pudo ser obtenida")

        if len(params_filtro) > 0:
            p_filtro = self.get_params(tipo=2, registro=params_filtro)
            if len(p_filtro) > 0:
                if "year" in p_filtro:
                    fechas["year"] = dia_t.year
                if "month" in p_filtro:
                    fechas["month"] = dia_t.month
                if "day" in p_filtro:
                    fechas["day"] = dia_t.day
                other_filters = [param for param in p_filtro if param not in ["year", "month", "day"]]
                if other_filters:
                    with open(self.ruta_config, 'r') as config_file:
                        config_data = json.load(config_file)
                        config_data_parameters = config_data["global"]["parametros_lz"]
                    for param in other_filters:
                        if param in config_data_parameters:
                            fechas[param] = config_data_parameters[param]
        return fechas
    
    def _definir_params_config(self,params):
        """
        Función privada que extrae los parametros de un archivo config.json y devuelve
        aquellos necesarios en la construcción del control en un diccionario para su posterior uso

        Args:
            self
            params (list): Lista de parametros a usar para la construcción del control
        Returns:
            params_def (dict): Diccionario con los valores de los parametros para la construcción del control
        """
        params_def = {}
        with open(self.ruta_config, 'r') as config_file:
            config_data = json.load(config_file)
            config_data_parameters = config_data["global"]["parametros_lz"]
        for param in params:
            if param in config_data_parameters:
                params_def[param] = config_data_parameters[param]
        return params_def

    def _construir_control(self,df=pd.DataFrame(),indice=""):
        """
        Función privada que extrae los parametros ingresados para el control
        para que sean empleados en la construcción del query

        Args:
            self
            df (pandas.DataFrame, optional): Dataframe con la información de los controles a ejecutar. Defaults to pd.DataFrame()
            indice (int, optional): Indice dentro de dataframe del control solicitado. Defaults to 0
        Returns:
            param_ctrl (dict): Diccionario con los valores de los parametros para la ejecución del control
        """
        param_ctrl ={}
        df=df
        indice=indice
        param_reg = df.at[indice,"params_reg"]
        p_ctrl = str(df.at[indice,"parametro_ctrl"])
        ctrl_name = df.at[indice,"ctrl"]
        nivel_name = df.at[indice, "nivel"]

        params_ctrl = self.get_params(tipo=2, registro=p_ctrl)

        if params_ctrl:
            params_ctrl_config = self._definir_params_config(params_ctrl)
            for param in params_ctrl_config:
                p_ctrl = p_ctrl.replace("{"+f"{param}"+"}",str(params_ctrl_config[param]))

        if "0" == p_ctrl:
            param_ctrl["sin"] = p_ctrl
            return param_ctrl
        
        if ctrl_name == 'nulls':
            ctrl = self._procesar_ctrl_nulls(p_ctrl, nivel_name)
            
        elif "|" in p_ctrl:
            return self._procesar_ctrl_pipe(p_ctrl, param_ctrl)
        
        elif ":" in p_ctrl:
            ctrl = p_ctrl.split(":",1)
        try:
            lista_reg = param_reg.split(",")
            for reg in lista_reg:
                if reg in ctrl:
                    param_ctrl[reg]=ctrl[1]
        except:
            raise self.log.list_error('query',1,ctrl_name)
        return param_ctrl
    
    def _procesar_ctrl_nulls(self, p_ctrl, nivel_name):
        """
        Función privada que define el comportamiento de controles de nulls

        Args:
            self
            p_ctrl (str): Valor de parametro_ctrl
            nivel_name (str): Nivel del control a construir
        Returns:
            param_ctrl (dict): Diccionario con los valores de los parametros para la ejecución del control
        """
        ctrl = p_ctrl.split(":", 1)
        if nivel_name == 'tabla':
            ctrl[1] = " OR ".join([f"{column} IS NULL OR CAST({column} as STRING) IN ('nan','inf') OR TRIM(CAST({column} as STRING)) = ''" for column in ctrl[1].split(",")])
        return ctrl
    
    def _procesar_ctrl_pipe(self, p_ctrl, param_ctrl):
        """
        Función privada que define el comportamiento de controles cuyo parametro_ctrl contenga |

        Args:
            self
            p_ctrl (str): Valor de parametro_ctrl
            param_ctrl (dict): Diccionario con los valores de los parametros para la ejecución del control
        Returns:
            param_ctrl (dict): Diccionario con los valores de los parametros para la ejecución del control actualizado
        """
        list_ctrl = p_ctrl.split("|")
        for n, n_ctrl in enumerate(list_ctrl):
            if ":" in n_ctrl:
                param_ctrl[f"{n}_"] = n_ctrl.split(":")[1]
            else:
                param_ctrl[f"{n}_"] = n_ctrl
        
        return param_ctrl
    
    def replace_params(self,query="",params=None):
        """
        Función que reemplaza los parametros obtenidos dentro del query
        para la ejecución del control solicitado

        Args:
            self
            query (str, optional): Query construido para la ejecución del control 
            params (dict, optional): Diccionario con los valores de parametros necesarios para la construcción del control
        Returns:
            param_ctrl (dict): Diccionario con los valores de los parametros para la ejecución del control
        """
        if params is None:
            params = {}
        txt_replace=query
        valido = True
        script=[]
        script_key=[]
        n=1
        while valido:
            script = self.get_params(tipo=3,registro=txt_replace)
            script_key = self.get_params(tipo=2,registro=txt_replace)
            if ((len(script)>0) and (n<3)) or ((len(script)>0) and (n<4) and ('filtro' not in query)):
                for p_query, key in zip(script,script_key):
                    if key not in ('fa','filtro_p'):
                        try:
                            txt_replace=txt_replace.replace(p_query,str(params[key]))
                        except Exception as e:
                            self.log.print_error("Error: al reemplazar el parametro: {} en el query: {} ".format(p_query,txt_replace))
                            return False
                        txt_replace=txt_replace.replace(p_query,str(params[key]))
                script=[]
                script_key=[]
                n=n+1
            else:
                valido=False
        return txt_replace


    