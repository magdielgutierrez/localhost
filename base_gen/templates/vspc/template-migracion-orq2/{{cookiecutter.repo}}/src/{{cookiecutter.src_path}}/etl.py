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
-- Descripción: Script de ejecución de los ETLs
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
"""
from orquestador2.step import Step
import time
import pkg_resources
import json

class Etl(Step):
    """
    Clase que representa el paso de ejecución de ETL's integrado en 
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
    

    def fn_Validar_Ingestiones(self):
        """
        Función vacia definida como ejemplo dentro de la clase Etl
        Args:
            kwargs con los parametros 
        """
        pass
                  

    def fn_ejecutar_etl(self):
        """
        Función que ejecuta secuencialmente cada una de las consultas 
        que fueron definidas dentro de la lista de rutas almancenadas
        en la variable archivos, que se encuentra establecida en el 
        diccionario ETL, que se encuentra estructurado dentro del archivo
        static\config.json.
        Args:
            kwargs con los parametros 
        """ 
        self.log.print_encabezado()
        for file in self.list_sql_file:
            archivo = self.getSQLPath() + file
            self.hp.ejecutar_archivo(archivo, self.params)
            
            
    def ejecutar(self):
        """
        Función heredada de la clase Step del orquestador 2, que define 
        los parámetros empleados dentro de los métodos establecidos dentro
        de la clase. Adcionalmente integra la ejecución de los métodos 
        definidos dentro de la lista de tareas que se encuentra dentro del
        diccionario ETL establecido en el archivo config.json
        """
        self.setGlobalConfig(self.initGlobalConfiguration())
        self.params = self.getGlobalConfiguration()["parametros_lz"] 
        self.hp = self.getHelper()
        self.list_sql_file=self.getStepConfig()["archivos"]
        self.executeTasks()