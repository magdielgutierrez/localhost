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
        Función que lee los parametros definidos dentro de la lista de tablas
        almacenadas en la variable ingestiones, que se encuentra establecida en el
        diccionario Preprocesador, que se encuentra estructurado dentro del archivo
        static\config.json.

        Una vez realiza la lectura, procesa los parametros y los convierte en una
        lista de diccionarios que será utilizada para obtener las ingestiones en el
        metodo fn_consultar_ingestiones, así como un diccionario de relaciones entre
        parametros para asignar los valores estos en el método fn_guardar_ingestiones

        Args:
            kwargs con los parametros 
        """
        
        #Preparar lista de diccionarios y relaciones
        lista_diccionarios = []
        relaciones = {}

        #Iterar en cada tabla para obtener sus parametros
        for tabla, valores in self.ingestiones.items():
            relaciones_temp = {}
            diccionario_temp = {
                "id_tabla":valores["id_tabla"],
                "zona": valores["zona"],
                "tabla": tabla,
            }

            campos = list(valores["campos"].items())
            
            for i in range(1, 4):
                campo_key = f"campo{i}"
                if i <= len(campos):
                    diccionario_temp[campo_key] = campos[i-1][1]
                    relaciones_temp[campos[i-1][0]] = campo_key
                else:
                    diccionario_temp[campo_key] = 0
            #Almacenar los parametros en la lista de diccionarios
            lista_diccionarios.append(diccionario_temp)
            #Almacenar las relaciones entre parametros
            relaciones[tabla]=relaciones_temp
            
        self.lista_dicts = lista_diccionarios
        self.relaciones_campos = relaciones
                  

    def fn_consultar_ingestiones(self):
        """
        Función que construye una tabla temporal para almacenar los datos
        de ingestiones, y posteriormente consulta secuencialmente dichos datos
        para cada una de las tablas que fueron definidas en la variable ingestiones,
        que se encuentra establecida en eldiccionario Preprocesador, que se 
        encuentra estructurado dentro del archivo static\config.json.
        Args:
            kwargs con los parametros 
        """ 
        # Crear tabla temporal para insertar ingestiones
        ruta_query_crear = self.getSQLPath() + "ingestiones/1_crear_ingestiones.sql"
        self.hp.ejecutar_archivo(ruta_query_crear,self.params) 

        # Ciclo para consultar ingestiones e insertarlo en la tabla temporal
        ruta_query_insertar=self.getSQLPath() + "ingestiones/2_insertar_ingestiones.sql"
        
        with open(ruta_query_insertar, 'r') as file:
            template_query = file.read()
            for dictionary in self.lista_dicts:
                text_query = template_query
                for key, value in dictionary.items():
                    text_query = text_query.replace('{' + key + '}', str(value))
                self.helper.ejecutar_consulta(text_query, self.params)
    
    def fn_guardar_ingestiones(self):
        """
        Función que consulta en la tabla temporal las ingestiones
        de cada tabla y luego las escribe en el archivo static\config.json,
        dentro de la clave parametros_lz, empleando el diccionario de parametros
        construido en fn_leer_parametros para asignar los valores correspondientes
        a cada uno de ellos.
        Args:
            kwargs con los parametros 
        """ 
        # Descargar tabla temporal con ingestiones
        ruta_query_obtener=self.getSQLPath() + "ingestiones/3_obtener_ingestiones.sql"
        df_ingestiones=self.hp.obtener_dataframe_archivo(ruta_query_obtener,self.params)


        # Construir diccionario de ingestiones
        dic_ingestiones = {}
        if 'tabla' in df_ingestiones.columns:
            for tabla, campos in self.relaciones_campos.items():
                for campo, valor in campos.items():
                    tabla_df = df_ingestiones.loc[df_ingestiones['tabla'] == tabla]
                    if not tabla_df.empty: 
                        dic_ingestiones[campo] = str(tabla_df[valor].iloc[0])

        # Guardar resultados
        ruta_json=pkg_resources.resource_filename(__name__, 'static/config.json')
        with open(ruta_json, 'r') as file:
            json_data = json.load(file)
            # Añadir el resultado al JSON existente
            json_data['global']['parametros_lz'].update(dic_ingestiones)
            
        with open(ruta_json, 'w') as file:
            json.dump(json_data, file, indent=4)
            
            
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