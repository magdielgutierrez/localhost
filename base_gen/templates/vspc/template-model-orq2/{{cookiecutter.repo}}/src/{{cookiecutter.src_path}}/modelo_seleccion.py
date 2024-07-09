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
import datetime
import warnings
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
warnings.filterwarnings('ignore')


class Modelo_seleccion:
    def __init__(self, df_entrenamiento = pd.DataFrame(), df_modelo = pd.DataFrame()):
        """
        Constructor de la Clase modelo_seleccion
        """
        self.df_modelo = df_modelo
        self.df_entrenamiento = df_entrenamiento

    def entrenamiento(self, df_entrenamiento = pd.DataFrame(), ruta = "", ruta_columnas = ""):
        """
        Función que se encarga de generar un archivo .pkl para predecir un comportamiento 
        Args:
            df_entrenamiento:  Dataframe con los datos de entrenamiento 
            ruta: ruta para respaldar .pkl seleccion del modelo en binario
            ruta_columnas: ruta para dejas archivo .xls con las columnas de seleccion
        Returns:
            Boolean: listado de querys del archivo config.json
            Tupla: Tupla con listado de las columnas de metricas
        """
        self.df_entrenamiento = df_entrenamiento
        metrics = []
        return True, metrics

    def ejecucion(self, df_modelo = pd.DataFrame(), ruta = "", ruta_columnas = ""):
        """
        Función que dado un archivo .pkl predice en comportamiento de los datos 
        Args:
            df_modelo:  Dataframe con los datos a predecir
            ruta: ruta para cargar .pkl seleccion del modelo en binario
            ruta_columnas: ruta para cargar archivo .xls con las columnas de seleccion
        Returns:
            List: listado de querys del archivo config.json
        """
        self.df_modelo = df_modelo
        df_respuesta = pd.DataFrame()
        return df_respuesta