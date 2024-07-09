# -*- coding: utf-8 -*-

"""
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- {{cookiecutter.vp_max}}
-----------------------------------------------------------------------------
-- Fecha Creación: {{cookiecutter.creation_date}}
-- Última Fecha Modificación: {{cookiecutter.creation_date}}
-- Autores: {{cookiecutter.package_author}}
-- Últimos Autores: {{cookiecutter.package_author}}
-- Descripción: Script de ejecución de los MODELOs
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
"""
import os
import json
import pkg_resources
from datetime	       		import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from orquestador2.step 	    import Step
from .modelo_seleccion import Modelo_seleccion



class Modelo(Step):
    """
    Clase encargada de la ejecución de los ETLs
    necesarios para extraer y procesar la información
    de interés de la rutina.
    """

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
    
    def fc_descargar_lz_df(self,indice=0):
        """
        Descarga el dataframe obtenido como resultado de ejecutar la consulta con el indice especificado.
        """
        self.log.print_encabezado()
        return self.sparky.helper.obtener_dataframe(consulta=self.list_sql_file[indice], params = self.params)
    
    def fc_subir_df_lz(self,tabla="training"):
        """
        Carga a la LZ la respuesta que genera la función ``predecir``.
        """
        query = "{}.{}".format(self.params["zona_r"], self.params_modelo["name_tablas"][tabla])
        k = 0
        reintento_df = True
        self.df_respuesta.to_excel("respuesta_modelo.xlsx")
        while reintento_df and k < 3:
            try:

                self.sparky.subir_df(
                    self.df_respuesta, query, modo = "overwrite")
                reintento_df = False
            except ReferenceError as erro:
                reintento_df = True
            k += 1
    
    def fc_training(self):
        """
        Se encarga del entrenamiento de un modelo X , verificando la fecha y validandola
        en una archivo de excel .
        Esta funcion opera con una instancia de la Clase Modelo_selección  
        Args:
            kwargs con los parametros desde el archivo de config.json
        """
        ruta_excel_entramiento = "{}{}".format(self.getSQLPath(), self.params_modelo["fechas_entrena_xlsx"])
        ruta_columnas = ""
        ruta = "{}/model/model.pkl".format(self.getSQLPath())
        if self.dia_entrenamiento == self.params_modelo["periodo"] :
            lista_fechas_entrena = pd.read_excel(ruta_excel_entramiento)
            if self.fecha_entrenamiento not in lista_fechas_entrena.fecha_entrena.tolist():
                # crear DataFrame con el día de actual
                df_fecha = pd.DataFrame(
                    {'fecha_entrena': [self.fecha_entrenamiento]})
                # Agregar dia actual al listado de fechas de entrenamiento ya realizados
                df_lista_fechas = pd.concat(
                    [lista_fechas_entrena, df_fecha], axis = 0, ignore_index = True)
                # guardar el archivo con las nuevas fechas de entrenamiento
                df_lista_fechas.to_excel(ruta_excel_entramiento, index = False)
                try:
                    df_entrenamiento = self.fc_descargar_lz_df()
                    entrenado, self.df_metricas = self.selmodelo.entrenamiento(
                        df_entrenamiento, ruta, ruta_columnas)  # Ejecutar entrenamiento
                except Exception as err:
                    entrenado = False
                    print('Error de entrenamiento: {}'.format(err))
                if not entrenado:
                    for index, row in lista_fechas_entrena.iterrows():
                        if lista_fechas_entrena.at[index, 'fecha_entrena'] == self.fecha_entrenamiento:
                            lista_fechas_entrena.at[index, 
                                                    'fecha_entrena'] = ''
                        lista_fechas_entrena.to_excel(
                            ruta_excel_entramiento, index = False)
        
    def fc_predecir(self):
        """
        Se encarga de obtener la respuesta del modelo , mediante la instancia de la clase
        ``Modelo Selección``.
        Args:
            kwargs con los parametros 
        """
        ruta_columnas = "{}/{}".format(
            self.obtener_ruta(), self.lista_rutas["columnas"])
        ruta = "{}/model/model.pkl".format(self.getSQLPath())
        df_modelo = self.fc_descargar_lz_df(indice=1)
        k = 0
        reintento_modelo = True
        while reintento_modelo and k < 3:
            try:
                self.df_respuesta = self.selmodelo.ejecucion(
                    df_modelo, ruta, ruta_columnas)
                if self.df_respuesta == None:
                    reintento_modelo = True
                reintento_modelo = False
                self.fc_subir_df_lz()
            except NotImplementedError as error:
                reintento_modelo = True
            k += 1
    
    def ejecutar(self):
        """
        Función heredada de la clase Step del orquestador 2, que define 
        los parámetros empleados dentro de los métodos establecidos dentro
        de la clase. Adcionalmente integra la ejecución de los métodos 
        definidos dentro de la lista de tareas que se encuentra dentro del
        diccionario Modelo establecido en el archivo config.json
        """
        self.setGlobalConfig(self.initGlobalConfiguration())
        self.params             = self.getGlobalConfiguration()["parametros_lz"]
        self.params_modelo      = self.getStepConfig()
        self.list_sql_file      = self.getStepConfig()["archivos"]
        self.hp                 = self.getHelper()
        self.iniciado           = False
        self.fecha_entrenamiento = datetime.today().strftime('%Y-%m-%d')
        self.dia_entrenamiento   = datetime.today().day
        self.mes_entrenamiento   = datetime.today().month
        self.df_model           = pd.DataFrame()
        self.selmodelo          = Modelo_seleccion()
        self.df_respuesta       = pd.DataFrame()
        self.df_metricas        = pd.DataFrame()
        self.df_metricas_modelo = pd.DataFrame()
        self.executeTasks()