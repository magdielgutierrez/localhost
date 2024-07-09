# -*- coding: utf-8 -*-

import json
import pkg_resources
import pandas as pd
import os


class Config_utils:
    """
    Clase principal de config_utils que se encarga de subir información a la LZ siguiendo buenas prácticas de código
    y lineamientos de la empresa. Tiene dos funciones principales que se encargan de subir la información a tablas
    historicas desde archivos de excel o dataframes
    """

    def __init__(self, ruta="", activar=True, sparky=object):
        """
        Constructor de la clase Config_utils, que recibe como base una instancia de la clase
        Sparky
        Args:
            ruta (str, optional): Ruta de la carpeta static. Defaults to "".
            activar (bool, optional): Activa o desactiva el Config-utils. Defaults to True.
            sparky (_type_, optional): Instancia de la clase Sparky. Defaults to Sparky().
        """
        self.create_insumo = self._obtener_sql("create")
        self.insert_insumo = self._obtener_sql("insert")
        self.df_collect = []
        self.inicio = activar
        self.sp = sparky
        if ruta == "":
            self.ruta = self.obtener_ruta()
        else:
            self.ruta = ruta

    def __obtener_json(self, ruta=""):
        """
        Función encargada de leer el archivo de configuración ubicado en
        {FilePath}/static/config.json y retornar un diccionario con la información
        del archivo

        Args:
            ruta (str, optional): Ruta de la carpeta static. Defaults to "".

        Returns:
            json: json con la información del archivo de configuración

        """
        path = "{}/{}".format(os.path.abspath(ruta), "config.json")
        if os.path.exists(path):
            with open(path) as f_in:
                json_str = f_in.read()

        return json.loads(json_str)

    def _obtener_sql(self, name=""):
        """
        Funición que se encarga de obtener la ruta de los archivos con las querys
        que se encuentran en la carpeta sql

        Args:
            name (str, optional): Nombre del archivo sql. Defaults to "".

        Returns:
            ruta: Ruta del archivo sql
        """

        ruta = "{}/sql/{}.sql".format(self.obtener_ruta(), name)
        return ruta

    def obtener_ruta(self):
        """
        Función que obtiene la ruta de la carpeta static

        Returns:
            ruta: String con la ruta completa hasta la carpeta static
        """
        return pkg_resources.resource_filename(__name__, "static")

    def __obtener_folder_xls(self, ruta_base=""):
        """
        Función encargada de obtener la ruta de los archivos de excel que se encuentran
        en la carpeta xlsx, además de obtener el nombre de las tablas que se van a crear
        en la LZ a partir de dichos archivos

        Args:
            ruta_base (str, optional): Ruta donde se encuentran los archivos de excel. Defaults to "".

        Returns:
            rutas_xls (list): Listado de rutas de los archivos
            lista_tbl_lz (list): Listado de nombres de las tablas
        """
        if self.inicio:
            # self.sp
            rutas_xls = []
            lista_tbl_lz = []
            if len(self.lista_name_xls) > 0:
                for root, dirs, files in os.walk(ruta_base):
                    for file in files:
                        for name_xls in self.lista_name_xls:
                            if file == name_xls:
                                ruta = r"{}/{}".format(root, file)
                                df_temp = pd.read_excel(ruta)
                                self.df_collect.append(df_temp)
                                rutas_xls.append(ruta)
                                lista_tbl_lz.append(
                                    r"{}".format(name_xls.split(".")[0])
                                )
                return rutas_xls, lista_tbl_lz

    def load_xlsx_lz(self, lista_xls=None, zona_r="", zona_p="proceso", ruta=""):
        """
        Función encargada de acondicionar la información requerida para realizar
        el proceso de carga de los archivos xlsx a la LZ a a partir de la función
        load_df_lz

        Args:
            lista_xls (list, optional): Listado de archivos xlsx a subir a la LZ. Defaults to [].
            zona_r (str, optional): Zona de resultados definida para la carga de los archivos en LZ. Defaults to "".
            zona_p (str, optional): Zona de proceso empleada para la carga de tablas intermedias. Defaults to "proceso".
        """

        if lista_xls is None:
            lista_xls = []
        name_xlsx = []
        ruta_json = ruta
        if ruta_json != "":
            params_json = self.__obtener_json(ruta_json)
            name_xlsx = ""
            if "global" in params_json:
                if "archivos" in params_json["global"]:
                    if "name_xlsx" in params_json["global"]["archivos"]:
                        name_xlsx = params_json["global"]["archivos"]["name_xlsx"]
            if len(name_xlsx) > 0:
                self.lista_name_xls = params_json["global"]["archivos"]["name_xlsx"]
            if "global" in params_json:
                if "parametros_lz" in params_json["global"]:
                    if "zona_p" in params_json["global"]["parametros_lz"]:
                        zona_p = params_json["global"]["parametros_lz"]["zona_p"]
                    if "zona_r" in params_json["global"]["parametros_lz"]:
                        zona_r = params_json["global"]["parametros_lz"]["zona_r"]
            self.zona = zona_r
            ruta = "{}/{}".format(os.path.abspath(ruta_json), "xlsx/")
            self.lista_rutas_xls, self.lista_tablas_lz = self.__obtener_folder_xls(ruta)
        else:
            self.lista_name_xls = lista_xls
            self.lista_rutas_xls, self.lista_tablas_lz = self.__obtener_folder_xls(ruta)
        self.load_df_lz(zona_p=zona_p, df_collection=self.df_collect, zona_r=zona_r)

    def load_df_lz(
        self,
        df=pd.DataFrame(),
        df_collection=None,
        zona_p="proceso",
        zona_r="",
        tabla="",
    ):
        """
        Función encargada de subir dataframes a la LZ siguiendo buenas prácticas de código

        Args:
            df (_type_, optional): Dataframe que se desa cargar en LZ. Defaults to pd.DataFrame().
            df_collection (list, optional): Listado de dataframes que se desean cargar en LZ. Defaults to [].
            zona_p (str, optional): Zona de proceso empleada para la carga de tablas intermedias. Defaults to "proceso".
            zona_r (str, optional): Zona de resultados definida para la carga de los archivos en LZ. Defaults to "".
            tabla (str, optional): Nombre con el que se va a cargar la tabla en LZ. Defaults to "".
        """
        if df_collection is None:
            df_collection = []
        self.df_collect = df_collection
        self.zona = zona_p
        if zona_r == "":
            self.zona_r = zona_p
        else:
            self.zona_r = zona_r
        if self.inicio:
            if len(self.df_collect) > 0 or len(df) > 0:
                if len(df) > 0 and self.df_collect == []:
                    self.df_collect.append(df)
                if len(self.zona) == 0:
                    self.zona = zona_p

                # for ruta_xlsx,nom_tabla in self.lista_rutas_xls,self.lista_tablas_lz:
                text = "INICIA CARGA  DF/XLSX-LZ"
                self.sp.logger.print()
                self.sp.logger.print_encabezado(text)
                self.sp.logger.info(text)
                ruta_file = "{}/consultas/0_params/insert_hist.sql".format(self.ruta)
                params = {}  # self.orquestador.preparador.params
                for index, df in enumerate(self.df_collect):
                    reintento = True
                    num_t = 0
                    while reintento and num_t < 3:
                        try:
                            reintento = False
                            if len(tabla) > 0:
                                params["tabla"] = tabla
                            else:
                                params["tabla"] = self.lista_tablas_lz[index]
                            # df.to_excel("{}.xlsx".format(self.lista_tablas_lz[index]))
                            self.sp.subir_df(
                                df,
                                nombre_tabla=params["tabla"],
                                zona=zona_p,
                                modo="overwrite",
                            )
                            query_describe = "DESCRIBE {}.{}".format(
                                zona_p, params["tabla"]
                            )
                            df_describe = self.sp.helper.obtener_dataframe(
                                query_describe
                            )
                            params["columns_name"], params["columns_type"] = (
                                self.__columns_namestypes(df_describe)
                            )
                            params_q = {
                                "zona_p": self.zona,
                                "zona_r": self.zona_r,
                                "tabla": params["tabla"],
                                "campos_tipo": ",".join(
                                    params["columns_type"].split(",")
                                ),
                                "campos": ",".join(params["columns_name"].split(",")),
                            }
                            self.sp.helper.ejecutar_archivo(
                                self.create_insumo, params_q
                            )
                            self.sp.helper.ejecutar_archivo(
                                self.insert_insumo, params_q
                            )
                        except ReferenceError as e:
                            self.sp.logger.error(msg=e)
                            reintento = True
                            num_t = num_t + 1
                text = "FINALIZA CARGA PARAMETROS XLSX-LZ"
                self.sp.logger.info(text)
                self.sp.logger.print()
        self.df_collect = []

    def val_hist_load_df_lz(
        self,
        df=pd.DataFrame(),
        df_collection=None,
        zona_p="proceso",
        zona_r="",
        tabla="",
    ):
        """
        Función encargada de subir dataframes a la LZ siguiendo buenas prácticas de código

        Args:
            df (_type_, optional): Dataframe que se desa cargar en LZ. Defaults to pd.DataFrame().
            df_collection (list, optional): Listado de dataframes que se desean cargar en LZ.
            Defaults to [].
            zona_p (str, optional): Zona de proceso empleada para la carga de tablas intermedias.
            Defaults to "proceso".
            zona_r (str, optional): Zona de resultados definida para la carga de los archivos en LZ. 
            Defaults to "".
            tabla (str, optional): Nombre con el que se va a cargar la tabla en LZ. Defaults to "".
        """
        if df_collection is None:
            df_collection = []
        self.zona = zona_p
        if zona_r == "":
            self.zona_r = zona_p
        else:
            self.zona_r = zona_r
        if self.inicio:
            if len(df_collection) > 0 or len(df) > 0:
                if len(df) > 0 and df_collection == []:
                    df_collection.append(df)

                text = "INICIA CARGA  DF/XLSX-LZ"
                self.sp.logger.print()
                self.sp.logger.print_encabezado(text)
                self.sp.logger.info(text)
                ruta_file = "{}/consultas/0_params/insert_hist.sql".format(self.ruta)
                params = {}

                for index, df_item in enumerate(df_collection):
                    reintento = True
                    num_t = 0
                    while reintento and num_t < 3:
                        try:
                            reintento = False
                            if len(tabla) > 0:
                                params["tabla"] = tabla
                            else:
                                params["tabla"] = self.lista_tablas_lz[index]
                            self.sp.subir_df(
                                df_item,
                                nombre_tabla=params["tabla"],
                                zona=zona_p,
                                modo="overwrite",
                            )
                            # Definicion de tipos de datos segun dataframe
                            query_describe_item = f"DESCRIBE {zona_p}.{params['tabla']}"
                            df_describe_item = self.sp.helper.obtener_dataframe(
                                query_describe_item
                            )
                            params["columns_name"], params["columns_type"] = (
                                self.__columns_namestypes(df_describe_item)
                            )

                            params_q = {
                                "zona_p": self.zona,
                                "zona_r": self.zona_r,
                                "tabla": params["tabla"],
                                "campos_tipo": ",".join(
                                    params["columns_type"].split(",")
                                ),
                                "campos": ",".join(params["columns_name"].split(",")),
                            }

                            self.sp.helper.ejecutar_archivo(
                                self.create_insumo, params_q
                            )

                            # Definicion de tipos de datos segun historico
                            query_describe_hist = f"DESCRIBE {zona_r}.{params['tabla']}"
                            df_describe_hist = self.sp.helper.obtener_dataframe(
                                query_describe_hist
                            )
                            params["hist_columns_name"], params["hist_columns_type"] = (
                                self.__columns_namestypes(df_describe_hist)
                            )

                            df_describe_hist, columnas_faltantes = self.__validar_estructura_tabla(
                                df_item, df_describe_hist
                            )

                            params["hist_columns_name"] = self.__insert_columns_cast(
                                df_describe_hist, columnas_faltantes
                            )
                            params_q = {
                                "zona_p": self.zona,
                                "zona_r": self.zona_r,
                                "tabla": params["tabla"],
                                "campos": ",".join(params["hist_columns_name"].split(",")),
                            }
                            self.sp.helper.ejecutar_archivo(
                                self.insert_insumo, params_q
                            )
                        except ReferenceError as e:
                            self.sp.logger.error(msg=e)
                            reintento = True
                            num_t = num_t + 1
                    del params["tabla"]
                text = "FINALIZA CARGA PARAMETROS XLSX-LZ"
                self.sp.logger.info(text)
                self.sp.logger.print()
        self.df_collect = []

    def __columns_namestypes(self, df=None):
        """
        Función encargada de consolidar los nombres y tipos de las columnas de un dataframe
        generado a partir de una query describe realizada sobre una tabla en LZ

        Args:
            df (_type_, optional): Dataframe generado con el resultado del describe. 
            Defaults to None.

        Returns:
            columns (string): Texto con los campos de la tabla generada con query de describe 
            formateados
            columns_name (string): Texto con los campos y tipo de datos para la tabla generada 
            con query de describe formateados
        """
        columns = ""
        columns_name = ""
        for index in df.index:
            if index == 0:
                columns_name = columns_name + "{} {}".format(
                    df.at[index, "name"], df.at[index, "type"]
                )
                columns = columns + "{}".format(df.at[index, "name"])
            else:
                columns_name = columns_name + ", {} {}".format(
                    df.at[index, "name"], df.at[index, "type"]
                )
                columns = columns + ", {}".format(df.at[index, "name"])
        return columns, columns_name

    def __insert_columns_cast(
        self, df_describe_hist: pd.DataFrame, columnas_faltantes: list
    ):
        """
        Función encargada de consolidar los nombres y tipos de las columnas de un dataframe
        generado a partir de una query describe sobre el historico de una tabla generada
        para solucionar el problema de tipo de dato para las columnas faltantes

        Args:
            df_describe_hist (pd.DataFrame): Dataframe generado con el resultado del describe 
            de la tabla histórica en LZ.
            columnas_faltantes (list): Lista de columnas faltantes en el dataframe generado 
            a partir del describe.

        Returns:
            hist_columns_name (string): Texto con los campos y tipo de datos para la 
            tabla generada con query de describe formateados
        """
        hist_columns_name = ""
        for index in df_describe_hist.index:
            columna = df_describe_hist.at[index, 'name']
            columna_alias = df_describe_hist.at[index, 'name']
            tipo_columna = df_describe_hist.at[index, 'type']
            if columna.lower() in columnas_faltantes:
                columna = "null"
            if index == 0:
                hist_columns_name = (
                    hist_columns_name
                    + f"cast({columna} as {tipo_columna}) as {columna_alias}"
                )
            else:
                hist_columns_name = (
                    hist_columns_name
                    + f", cast({columna} as {tipo_columna}) as {columna_alias}"
                )
        return hist_columns_name

    def __validar_estructura_tabla(
        self, df_item:pd.DataFrame, df_hist:pd.DataFrame
    ):
        """
        Valida la estructura de la tabla y realiza ajustes si es necesario.

        Args:
            df_item (pd.DataFrame): DataFrame con la información a cargar en la tabla.
            df_hist (pd.DataFrame): DataFrame con la estructura histórica de la tabla.

        Raises:
            self.sp.logger.error: Se genera un error si la información procesada contiene columnas 
            que no hacen parte del histórico.

        Returns:
            pd.DataFrame: DataFrame con la estructura ajustada de la tabla.
            list: Lista de columnas faltantes en el DataFrame de la tabla histórica.
        """
        drop_col = [
            "ingestion_year",
            "ingestion_month",
            "ingestion_day",
            "fecha_ejecucion",
            "year",
        ]
        df_item = df_item.drop(drop_col, axis=1, errors="ignore")
        df_hist = df_hist[~df_hist.name.isin(drop_col)].reset_index()
        if set(df_item.columns.values).issubset(df_hist.name.values):
            return df_hist, list(
                set(df_hist.name.values) - set(df_item.columns.values)
            )
        raise self.sp.logger.error(
            msg="la informacion procesada tiene columnas que no hacen parte del historico"
        )
