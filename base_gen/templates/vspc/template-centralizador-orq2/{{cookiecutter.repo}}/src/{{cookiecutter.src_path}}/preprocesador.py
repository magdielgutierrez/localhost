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
-- Descripción: Script de ejecución del Preprocesamiento
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
"""
# -*- coding: utf-8 -*-

"""
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- Vicepresidencia de Servicios para los Clientes
-----------------------------------------------------------------------------
-- Fecha Creación: 20230921
-- Última Fecha Modificación: 20230921
-- Autores: USERNAME1
-- Últimos Autores: USERNAME1
-- Descripción: Script de ejecución del Preprocesamiento
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
"""
from orquestador2.step import Step
import time
import pkg_resources
import json

class Preprocesador(Step):
    """
    Clase que representa el paso de validación de datos integrado en 
    la base calendrizable.
    """
    
    def obtener_ruta(self):
        """
        Función que obtiene la ruta absoluta de la carpeta static
        contenida dentro del proyecto
        Args:
            kwargs con los parametros 
        """
        return pkg_resources.resource_filename(__name__, 'static') 
    

    def fn_leer_parametros(self):
        """
        Función que lee los parametros definidos dentro de la tabla parametrica
        de consulta de ingestiones y los guarda como dataframe para su posterior
        uso
        Args:
            kwargs con los parametros 
        """

        # Consultar la tabla central de parametros de ingestiones
        ruta_query_crear = self.getSQLPath() + "ingestiones/3_obtener_ingestiones.sql"
        with open(ruta_query_crear, 'r') as file:
            template_query = file.read()
            text_query = template_query.replace('{zonatabla}','proceso.{{cookiecutter.index}}_ingestion_params')
            self.df_ingestiones = self.hp.obtener_dataframe(text_query)
                  

    def fn_consultar_ingestiones(self):
        """
        Función que construye una tabla para almacenar los datos
        de ingestiones, y posteriormente consulta secuencialmente dichos datos
        para cada una de las tablas que fueron definidas en la tabla parametrica
        Args:
            kwargs con los parametros 
        """ 
        # Crear tabla para insertar ingestiones en caso de que no exista
        ruta_query_crear = self.getSQLPath() + "ingestiones/1_crear_ingestiones.sql"
        self.hp.ejecutar_archivo(ruta_query_crear,self.params) 

        # Ciclo para consultar ingestiones e insertarlo en la tabla
        ruta_query_insertar=self.getSQLPath() + "ingestiones/2_insertar_ingestiones.sql"
        
        self.df_ingestiones = self.df_ingestiones.fillna(0)

        with open(ruta_query_insertar, 'r') as file:
            template_query = file.read()
            for _, row in self.df_ingestiones.iterrows():
                text_query = template_query.replace('{campo1}', str(row['campo1']))
                text_query = text_query.replace('{campo2}', str(row['campo2']))
                text_query = text_query.replace('{campo3}', str(row['campo3']))
                text_query = text_query.replace('{tabla}', str(row['tabla']))
                self.helper.ejecutar_consulta(text_query, self.params)
            
            
    def ejecutar(self):
        """
        Función heredada de la clase Step del orquestador 2, que define 
        los parámetros empleados dentro de los métodos establecidos dentro
        de la clase. Adcionalmente integra la ejecución de los métodos 
        definidos dentro de la lista de tareas que se encuentra dentro del
        diccionario ETL establecido en el archivo config.json
        """
        self.params = self.getGlobalConfiguration()["parametros_lz"] 
        self.hp = self.getHelper()
        self.ingestiones = self.getStepConfig()["ingestiones"]
        self.executeTasks()