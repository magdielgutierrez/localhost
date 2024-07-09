# -*- coding: future_fstrings -*-

"""
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- Equipo Vicepresidencia de Servicios para los Clientes
-----------------------------------------------------------------------------
-- Fecha Creación: 20221110
-- Última Fecha Modificación: 20221110
-- Autores: ancallej
-- Últimos Autores: ancallej
-- Descripción: Script de ejecución de los ETLs
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
"""
from datetime	       		import datetime
from dateutil.relativedelta import relativedelta
import json
import pkg_resources
import os
import pandas as pd


class Respaldo_logs():
    """
    Clase encargada de la ejecución de los ETLs
    necesarios para extraer y procesar la información
    de interés de la rutina.
    """

    def __init__(self,sparky=object,activate=True,historica=True,mostrar_df=False,nom_tabla="",zona_p="",zona_r="",proyecto="",ruta=""):
        """_summary_

        Args:
            sparky (_type_ Sparky-bc): _description_. Defaults to object.

            activate (bool, optional): _description_. Defaults to True.

            historica (bool, optional): _description_. Defaults to True.

            mostrar_df (bool, optional): _description_. Defaults to False.

            nom_tabla (str): _description_. si es vacio ("") inactiva la ejecucion del respaldo activate=False.

            zona_p (str): _description_. Defaults to "proceso".

            zona_r (str, optional): _description_. si no se asigna la zora de resultados inactiva el respaldo historico.

            proyecto (str, optional): nombre del componente desplegado en artifactory.

            ruta (str): _description_. si es vacio ("") Si este vacío no consulta el JSON de lo contrario trae los parámetro de configuración.
        """
        if len(ruta) > 0:
            params_json = self.__obtener_json(ruta)
            if "global" in params_json:
                if "parametros_lz" in params_json["global"]:
                    if "nom_tablas" in params_json["global"]["parametros_lz"]:
                        if "logs" in params_json["global"]["parametros_lz"]['nom_tablas']:
                            nom_tabla = params_json["global"]["parametros_lz"]['nom_tablas']["logs"]
                    if "zona_p" in params_json["global"]["parametros_lz"]:
                        zona_p = params_json["global"]["parametros_lz"]["zona_p"]
                    if "zona_r" in params_json["global"]["parametros_lz"]:
                        zona_r = params_json["global"]["parametros_lz"]["zona_r"]
                    if "nom_proyecto" in params_json["global"]["parametros_lz"]:
                        proyecto = params_json["global"]["parametros_lz"]["nom_proyecto"]

        self.sp= sparky
        self.activate=activate
        self.historica=historica
        self.mostrar_df = mostrar_df
        self.df_monitoreo = pd.DataFrame()
        self.count_logs= self.obtener_querys("count_logs")
        self.create_logs= self.obtener_querys("create_logs")
        self.insert_logs= self.obtener_querys("insert_logs")
        self.params= self.obtener_params()
        if proyecto=="":
            self.params["proyecto"]="no-definido"
        else:
            self.params["proyecto"]=proyecto
        if nom_tabla=="":
            self.activate=False
        else:
            self.params["nom_tabla"]=nom_tabla
        if zona_p == "":
            self.params["zona_p"]="proceso"
        else:
            self.params["zona_p"]=zona_p
        if zona_r=="":
            self.historica=False
        else:
            self.params["zona_r"]=zona_r
            self.params["historica"]="{}.{}".format(self.params["zona_r"],self.params["nom_tabla"])
        
        self.__logs_ejecucion()

    @staticmethod
    def obtener_ruta():
        """
        Función encargada de identificar la
        carpeta static relacionada al paquete
        ------
        Return
        ------
        ruta_src : string
        Ruta static en el sistema o entorno de
        los recursos del paquete
        """
        return pkg_resources.resource_filename(__name__, 'static')

    def obtener_params(self):
        fecha_ejecucion = datetime.utcnow()
        params = {
            "year":"{}".format(fecha_ejecucion.year),
            "month":"{}".format(fecha_ejecucion.month),
            "day":"{}".format(fecha_ejecucion.day),
            "nom_tabla":"",
            "zona_p":"",
            "zona_r":"",
            "historica":""
        }
        return params

    def obtener_querys(self, nombre_arch=""):
        """Obtiene consulta SQL de un archivo

        Args:
            nombre_arch (str, optional): Nombre del archivo en carpeta static/sql. Defaults to "".

        Returns:
            str: Consulta SQL del archivo
        """
        ruta_static = self.obtener_ruta()
        ruta = "{}/sql/{}.sql".format(ruta_static,nombre_arch)
        with open(f'{ruta}') as infile:
            query = infile.read()
        return query

    def __obtener_json(self,ruta=""):
        """Lee el archivo de configuración ubicado en {FilePath}/static/config.json

        Returns
        -------
        None.

        """
        path = "{}/{}".format(os.path.abspath(ruta),"config.json")

        if os.path.exists(path):
            with open( path ) as f_in :
                json_str = f_in.read()

        return json.loads( json_str )

    def __logs_ejecucion(self):
        """_summary_
        """
        if self.activate:
            try:
                self.df_monitoreo = self.sp.logger.df
                self.df_monitoreo['cant_registros'] = '0'
                self.df_monitoreo['proyecto'] = self.params["proyecto"]
                text = 'RESPALDO LOGS'
                self.sp.logger.print(text)
                self.sp.logger.print_encabezado()
                cantidad = len(self.sp.logger.df)
                param_zonatabla={"zona_tabla":""}
                if cantidad != 0:
                    for index, row in self.df_monitoreo.iterrows():
                        try:
                            if self.df_monitoreo.at[index, 'tipo'] == 'CREATE' or self.df_monitoreo.at[index, 'tipo'] == 'INSERT':
                                if self.df_monitoreo.at[index, 'nombre'] != '' and self.df_monitoreo.at[index, 'estado'] != 'pendiente' and self.df_monitoreo.at[index, 'estado'] != 'error':
                                    param_zonatabla["zona_tabla"]=self.df_monitoreo.at[index, 'nombre']
                                    if not any(chr.isdigit() for chr in param_zonatabla["zona_tabla"]):
                                        querys_count=self.count_logs.format(param_zonatabla["zona_tabla"])
                                        print("query : {}".format(querys_count))
                                        try:
                                            cant_reg =  self.sp.helper.obtener_dataframe(querys_count)
                                            print("count : {}".format(cant_reg.iloc[0, 0]))
                                            self.df_monitoreo.at[index, 'cant_registros'] = cant_reg.iloc[0, 0]
                                            self.sp.logger.df.loc[cantidad,'etapa'] = "Respal Logs"
                                            self.sp.logger.df.loc[cantidad,'sub_i'] = 0
                                            self.sp.logger.df.loc[cantidad,'nombre'] = self.df_monitoreo.at[index, 'nombre']
                                            self.sp.logger.df.loc[cantidad,'documento'] = self.df_monitoreo.at[index, 'documento']
                                            self.sp.logger.df.loc[cantidad,'tipo'] = "SELECT"
                                            self.df_monitoreo.loc[cantidad]=self.sp.logger.df.loc[cantidad]
                                            self.df_monitoreo.at[cantidad,'cant_registros']=cant_reg.iloc[0, 0]
                                            cantidad = cantidad + 1

                                        except Exception as e:

                                            self.sp.logger.df.loc[cantidad,'etapa'] = "Respal Logs"
                                            self.sp.logger.df.loc[cantidad,'sub_i'] = 0
                                            self.sp.logger.df.loc[cantidad,'nombre'] = self.df_monitoreo.at[index, 'nombre']
                                            self.sp.logger.df.loc[cantidad,'documento'] = self.df_monitoreo.at[index, 'documento']
                                            self.sp.logger.df.loc[cantidad,'tipo'] = "SELECT"
                                            self.df_monitoreo.loc[cantidad]=self.sp.logger.df.loc[cantidad]
                                            self.df_monitoreo.at[cantidad,'cant_registros']=-1
                                            self.df_monitoreo.loc[cantidad]=self.sp.logger.df.loc[cantidad]
                                            cantidad = cantidad + 1
                                            self.df_monitoreo.at[index, 'cant_registros'] = '-1'

                        except ReferenceError as err:
                            self.sp.logger.df.loc[cantidad,'etapa'] = "Respal Logs"
                            self.sp.logger.df.loc[cantidad,'sub_i'] = 0
                            self.sp.logger.df.loc[cantidad,'nombre'] = self.df_monitoreo.at[index, 'nombre']
                            self.sp.logger.df.loc[cantidad,'documento'] = self.df_monitoreo.at[index, 'documento']
                            self.sp.logger.df.loc[cantidad,'tipo'] = "SELECT"
                            self.df_monitoreo.loc[cantidad]=self.sp.logger.df.loc[cantidad]
                            self.df_monitoreo.at[cantidad,'cant_registros']=-1
                            self.df_monitoreo.loc[cantidad]=self.sp.logger.df.loc[cantidad]
                            cantidad = cantidad + 1
                            self.df_monitoreo.at[index, 'cant_registros'] = '-1'
                    self.df_monitoreo.drop(['consulta'], axis = 1, inplace = True)
                    k = 0
                    reintento_df = True
                    self.df_monitoreo["hora_inicio"]=self.df_monitoreo["hora_inicio"].astype(str).str.replace("-","/")
                    self.df_monitoreo['year'] = int(self.params["year"])
                    self.df_monitoreo['ingestion_year'] = int(self.params["year"])
                    self.df_monitoreo['ingestion_month'] = int(self.params["month"])
                    self.df_monitoreo['ingestion_day'] = int(self.params["day"])
                    if self.mostrar_df:
                        self.df_monitoreo.to_excel("respaldo_logs.xlsx")
                    zona_tabla = "{}.{}".format(self.params["zona_p"],self.params["nom_tabla"])
                    self.archivos_id = self.sp.logger.establecer_tareas(
                        "", 'RESPALDO LOGS', zona_tabla)
                    while reintento_df and k < 3:
                        for arch_id in self.archivos_id:
                            try:
                                self.sp.logger.iniciar_tarea(arch_id)
                                self.sp.subir_df(
                                    self.df_monitoreo, zona_tabla, modo = "overwrite")
                                self.sp.logger.finalizar_tarea(arch_id)
                                reintento_df = False
                            except IndexError as erro:
                                self.sp.logger.error_query(arch_id, erro, False)
                                reintento_df = True
                            k += 1
                    if self.historica:
                        self.sp.helper.ejecutar_consulta(self.create_logs.format(self.params["historica"]))
                        self.sp.helper.ejecutar_consulta(self.insert_logs.format(self.params["historica"],zona_tabla))
                    self.sp.logger.finalizar(groupby = ['etapa', 'documento'])
                    self.sp.logger.log.info(
                        ' --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  -- ')
                    self.sp.logger.log.info(
                        '     Finaliza respaldo de Logs             ')
                    self.sp.logger.log.info(
                        ' --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  -- ')
            except Exception as e:
                self.sp.logger.log.error(
                ' --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  -- ')
                self.sp.logger.log.error(
                    '     Error al ejecutar modulo Respaldo-Logs             ')
                self.sp.logger.log.error(
                    ' --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  -- ')