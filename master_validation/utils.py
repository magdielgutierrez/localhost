import pandas as pd
import pkg_resources

class Utils:
    '''
    Clase que contiene funciones de utilidad para el maestro de validaciones
    '''

    @staticmethod
    def eliminar_columnas(df):
        """
        Función que elimina las columnas de ingestión del dataframe con la información
        de los controles
        Args:
            self
            df (pandas.DataFrame): Dataframe con los controles
        Returns:
            df (pandas.DataFrame): Dataframe con los controles sin las columnas de ingestión
        """
        cols_to_drop = ["ingestion_year", "ingestion_day", "ingestion_month"]
        cols_to_drop = [col for col in cols_to_drop if col in df.columns]
        df = df.drop(cols_to_drop, axis=1)
        return df
    
    @staticmethod
    def _columns_namestypes(df):
        '''
        Función que obtiene el nombre de las columnas de un dataframe
        Args:
            df (pandas.DataFrame): Dataframe con los controles
        Returns:
            columns (str): string con los nombres de las columnas del dataframe
        '''
        columns = "A."
        for index  in df.index:
            if index == 0:
                columns = columns + "{}".format(df.at[index,"name"])
            else:
                columns = columns + ", A.{}".format(df.at[index,"name"])
        return columns
    
    @staticmethod
    def _obtener_ruta():
        """
        Función Privada que arroja la ruta a la carpeta static.
        Returns:
            ruta (str): ruta de la carpeta static.
        """
        return pkg_resources.resource_filename(__name__, 'static')
    
    @staticmethod    
    def _describir_tabla(zona_tabla_output: str = '', sparky = object, logger = object):
        """
        Función que describe la tabla de resultados del maestro de validaciones
        Args:
            self
            zona_tabla_output (str): Nombre de la tabla de resultados del maestro de validaciones
            sparky (object): Objeto de la clase Sparky
            logger (object): Objeto de la clase Logger
        Returns:
            df_describe (pandas.DataFrame): Dataframe con la descripción de la tabla de resultados
        """
        query_describe = "DESCRIBE {}".format(zona_tabla_output)
        try:
            df_describe=sparky.helper.obtener_dataframe(query_describe)
        except Exception as e:
            if 'AnalysisException: Could not resolve path:' in str(e):
                return pd.DataFrame()
            else:
                raise logger.list_error('master_val',1)
        return df_describe