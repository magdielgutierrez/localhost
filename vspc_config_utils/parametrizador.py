import pkg_resources
import pandas as pd
from vspc_config_utils.config_utils import Config_utils
import json
from datetime import date
import math


class Parametrizador:
    """
    Clase auxiliar de config_utils que se encarga de la verificacion de la calidad en los insumos
    y las buenas practicas a la hora de subir alguna tabla a la lz
    """

    def __init__(self, sparky, indice, zona, ruta, zona_p="proceso"):
        """
        Args:
            sparky (object): Instancia de Sparky.
            indice (str): Indice por el cual se denotara la tabla a crear.
            zona (str): Zona en la cual el usuario quiere que sea subida la información.
            ruta (str): Ruta en la cual se encuentra la informacion que dispone para subir a la lz.
            zona_p (str, optional): Zona en la cual se subira las tablas parametricas. Defaults to 'proceso'.
        """
        self.indice = indice
        self.zona = zona
        self.ruta = ruta
        self.sp = sparky
        self.zona_p = zona_p
        self.strlimit = 200000

    def obtener_tablas_parametricas(self, tabla="", df=False):
        """
        Función para generar las tablas parametricas en la zona de procesos indicada

        Args:
            zona (str): El nombre de la zona.
            indice (str): El nombre del índice.

        Returns:
            boolean: True si pudo subir las tablas, False si no fue posible subirlas.
        """
        params = {"zona": self.zona, "indice": self.indice}
        query_describe = "DESCRIBE {}.{}_central_params".format(self.zona, self.indice)
        try:
            df_describe = self.sp.helper.obtener_dataframe(query_describe)
        except Exception as e:
            if "Could not resolve path" in str(e):
                self.sp.logger.error(
                    msg="no existe la tabla central en la LZ, no se pueden generar las tablas parametricas"
                )
                return False
            else:
                raise e
        params["columns_name"] = self.columns_namestypes(df_describe)
        ruta_select = "{}/sql/select.sql".format(
            pkg_resources.resource_filename(__name__, "static")
        )
        with open(ruta_select) as in_file_sql:
            select_sql = in_file_sql.read()
        df_lz = self.sp.helper.obtener_dataframe(consulta=select_sql, params=params)
        df_lz_clean = self.eliminar_columnas(df_lz)
        df_lz_clean = self.unificar_registros(df_lz_clean)

        if df_lz_clean.empty:
            self.sp.logger.error(
                msg="no hay registros para la tabla central, no se pueden generar las tablas parametricas"
            )
            return False

        try:
            list_df = []
            if isinstance(tabla, str):
                if tabla == "":
                    for _, row in df_lz_clean.iterrows():
                        json_data = json.loads(
                            row["json_tabla"]
                            .replace("'", '"')
                            .replace(" nan,", " null,")
                            .replace(" nan]", " null]")
                            .replace("[nan,", "[null,")
                            .replace(' " ', " ' ")
                            .replace("\\", "")
                            .replace("' ", '"')
                        )
                        df_json = pd.DataFrame(json_data)
                        df_json = df_json.applymap(
                            lambda x: x.replace(" ' ", "'") if isinstance(x, str) else x
                        )
                        if df:
                            list_df.append(df_json)
                        else:
                            nombre_tabla = str(row["tabla"])
                            self.subir_tabla_parametrica(
                                df_json, nombre_tabla, self.zona_p
                            )
                else:
                    df_filtrado = df_lz_clean[df_lz_clean["nombre"] == tabla]
                    json_data = json.loads(
                        df_filtrado.iloc[0]["json_tabla"]
                        .replace("'", '"')
                        .replace(" nan,", " null,")
                        .replace(" nan]", " null]")
                        .replace("[nan,", "[null,")
                        .replace(' " ', " ' ")
                        .replace("\\", "")
                        .replace("' ", '"')
                    )
                    df_json = pd.DataFrame(json_data)
                    df_json = df_json.applymap(
                        lambda x: x.replace(" ' ", "'") if isinstance(x, str) else x
                    )
                    if df:
                        return df_json
                    else:
                        nombre_tabla = str(df_filtrado.iloc[0]["tabla"])
                        self.subir_tabla_parametrica(df_json, nombre_tabla, self.zona_p)
            elif isinstance(tabla, list):
                for nombre_tabla in tabla:
                    df_filtrado = df_lz_clean[df_lz_clean["nombre"] == nombre_tabla]
                    json_data = json.loads(
                        df_filtrado.iloc[0]["json_tabla"]
                        .replace("'", '"')
                        .replace(" nan,", " null,")
                        .replace(" nan]", " null]")
                        .replace("[nan,", "[null,")
                        .replace(' " ', " ' ")
                        .replace("\\", "")
                        .replace("' ", '"')
                    )
                    df_json = pd.DataFrame(json_data)
                    df_json = df_json.applymap(
                        lambda x: x.replace(" ' ", "'") if isinstance(x, str) else x
                    )
                    if df:
                        list_df.append(df_json)
                    else:
                        nombre_tabla = str(df_filtrado.iloc[0]["tabla"])
                        self.subir_tabla_parametrica(df_json, nombre_tabla, self.zona_p)
            if df:
                return list_df
            else:
                return True
        except Exception as e:
            self.sp.logger.error(msg="error al subir las tablas parametricas")
            self.sp.logger.error(msg=e)
            raise e

    def subir_tabla_parametrica(self, df, nombre, zona_p):
        """
        Función que se encarga de subir un dataframe a una zona de procesos de la LZ, incluyendo
        las columnas de ingestión dia, mes, año y fecha_ejecucion

        Args:
            df (pandas.DataFrame): Dataframe con la información a subir
            nombre (str): Nombre de la tabla a subir
            zona_p (str): Zona en la cual se subira la tabla
        """
        dia_actual = date.today()
        df["ingestion_year"] = dia_actual.year
        df["ingestion_month"] = dia_actual.month
        df["ingestion_day"] = dia_actual.day
        df["fecha_ejecucion"] = dia_actual
        self.sp.subir_df(df, nombre_tabla=nombre, zona=zona_p, modo="overwrite")
        msg = f"La tabla {nombre} ha sido subida a la zona {zona_p}"
        self.sp.logger.info(msg=msg)

    def subir_tabla_central(self):
        """
        Función que realiza la parametrización de datos a partir de un archivo Excel y los compara con la tabla central.

        Esta función toma datos de un archivo Excel y los compara con una tabla central en una base de datos utilizando la
        clase Sparky.

        Luego sube la información del archivo a la tabla central de datos

        Args:
            indice (str): El nombre del índice que se utilizará para la parametrización.
            zona (str): El nombre de la zona a la que pertenecen los datos.
            ruta (str): La ruta al archivo Excel que contiene los datos de parametrización.
            sp (Sparky): Una instancia de la clase Sparky utilizada para acceder a la base de datos.

        Returns:
            None

        """
        datos_central = self.generacion_datos()

        if self.validar_parametros():
            df_entrada = pd.DataFrame(datos_central)
            c_util = Config_utils(sparky=self.sp)
            self.actualizar_informacion(df_entrada, c_util)

    def actualizar_informacion(self, df_entrada, c_util):
        """
        Actualiza la tabla central según sea necesario.

        Args:
            df_entrada (pandas.DataFrame): El DataFrame de entrada.
            datos_individuales (list): Una lista de datos individuales.
            zona (str): El nombre de la zona.
            indice (str): El nombre del índice.
            sp (Sparky): Una instancia de la clase Sparky.

        Returns:
            None
        """
        equality, existence = self.comparar_tablas_centrales(df_entrada)

        if existence:
            if equality:
                self.sp.logger.info(
                    msg="no hay cambios en las tablas parametricas, no es necesaria una actualizacion"
                )
            else:
                # Actualizar toda la tabla central
                c_util.load_df_lz(
                    df=df_entrada,
                    zona_r=self.zona,
                    tabla="{}_central_params".format(self.indice),
                )

        else:
            # Actualizar toda la tabla central
            c_util.load_df_lz(
                df=df_entrada,
                zona_r=self.zona,
                tabla="{}_central_params".format(self.indice),
            )

    def comparar_tablas_centrales(self, df_entrada):
        """
        Compara un DataFrame de entrada con una tabla central y determina si hay diferencias.

        Args:
            zona (str): El nombre de la zona.
            indice (str): El nombre del índice.
            df_entrada (pandas.DataFrame): El DataFrame de entrada a comparar con la tabla central.

        Returns:
            tuple: Una tupla que contiene dos elementos, donde el primero es una lista de índices con diferencias
            si las hay.
        """
        df_lz = self.obtener_dataframe()
        if df_lz is not None:
            return df_entrada.equals(df_lz), True
        else:
            return False, False

    def validar_parametros(self):
        """
        Valida los parámetros 'zona' e 'indice' antes de procesarlos.

        Args:
            zona (str): El nombre de la zona a validar.
            indice (str): El nombre del índice a validar.

        Returns:
            bool: True si los parámetros son válidos, False si no lo son.
        """
        if len(self.zona) < 8 or any(char.isdigit() for char in self.zona):
            self.sp.logger.error(
                msg="El parámetro 'zona' debe tener al menos 8 caracteres y no debe incluir números."
            )
            return False
        if len(self.indice) > 10 or any(char.isdigit() for char in self.indice):
            self.sp.logger.error(
                "El parámetro 'indice' debe tener como máximo 8 caracteres y no debe incluir números."
            )
            return False
        return True

    def obtener_dataframe(self):
        """
        Obtiene un DataFrame de la tabla central de la zona e índice especificados.

        Args:
            zona (str): El nombre de la zona.
            indice (str): El nombre del índice.

        Returns:
            pandas.DataFrame or None: Un DataFrame con los datos de la tabla central si existe, None si no existe.
        """
        params = {"zona": self.zona, "indice": self.indice}
        query_describe = "DESCRIBE {}.{}_central_params".format(self.zona, self.indice)
        try:
            df_describe = self.sp.helper.obtener_dataframe(query_describe)
        except Exception as e:
            if "Could not resolve path" in str(e):
                self.sp.logger.info(
                    msg="no existe la tabla en la LZ, continuando con el parametrizador"
                )
                return None
            else:
                raise e
        params["columns_name"] = self.columns_namestypes(df_describe)
        ruta_select = "{}/sql/select.sql".format(
            pkg_resources.resource_filename(__name__, "static")
        )
        with open(ruta_select) as in_file_sql:
            select_sql = in_file_sql.read()
        df_lz = self.sp.helper.obtener_dataframe(consulta=select_sql, params=params)

        return self.eliminar_columnas(df_lz)

    def eliminar_columnas(self, df):
        """
        Función que elimina las columnas de ingestión del dataframe con la información
        de los controles

        Args:
            df (pandas.DataFrame): Dataframe con los controles
        """
        cols_to_drop = [
            "ingestion_year",
            "ingestion_day",
            "ingestion_month",
            "fecha_ejecucion",
            "year",
        ]
        cols_to_drop = [col for col in cols_to_drop if col in df.columns]
        df = df.drop(cols_to_drop, axis=1)
        return df

    def unificar_registros(self, df):
        """
        Función que unifica los los registros del campo json_tabla, separados previamente para
        su correcta carga en la LZ, garantizando el correcto funcionamiento de parametrizador con
        la limitación de la variable string columns length ODBC en productivo

        Args:
            df (pandas.DataFrame): Dataframe con los catalogos
        """
        patron = r"_p\d+$"
        patronr = r"(_p\d+$)"
        cat_sep = (
            df[df["tabla"].str.contains(patron)]["tabla"]
            .str.split(patron)
            .str.get(0)
            .drop_duplicates(inplace=False)
            .tolist()
        )
        cat_un = (
            df[~(df["tabla"].str.contains(patron))]["tabla"]
            .str.split(patron)
            .str.get(0)
            .drop_duplicates(inplace=False)
            .tolist()
        )
        df_res = df[df["tabla"].isin(cat_un)].reset_index(drop=True)

        for cat in cat_sep:
            pat_cat = cat + patron
            sub_cat = df[df["tabla"].str.contains(pat_cat)].reset_index(drop=True)
            sub_cat["orden"] = sub_cat["tabla"].str.extract(patronr)
            sub_cat["orden"] = (
                sub_cat["orden"].str.split("p").str.get(1).astype("int64")
            )
            sub_cat.sort_values("orden", inplace=True)
            reg = sub_cat.iloc[0]
            reg = reg.drop("orden")
            reg["tabla"] = cat
            reg["json_tabla"] = "".join(sub_cat["json_tabla"].tolist().copy())
            df_res = pd.concat([df_res, pd.DataFrame([reg])], ignore_index=True)
        return df_res

    def columns_namestypes(self, df=None):
        """
        Función privada que genera un string con las columnas del dataframe de controles

        Args:
            df (pandas.DataFrame, optional): Dataframe de controles. Defaults to None

        Returns:
            columns (str)
        """
        columns = "A."
        for index in df.index:
            if index == 0:
                columns = columns + "{}".format(df.at[index, "name"])
            else:
                columns = columns + ", A.{}".format(df.at[index, "name"])
        return columns

    def generacion_datos(self):
        """
        Llena la lista datos_central con datos del archivo Excel.

        Args:
            indice (str): El nombre del índice.
            xls (pd.ExcelFile): El archivo Excel que contiene los datos de parametrización.
            zona (str): El nombre de la zona a la que pertenecen los datos.

        Returns:
            list: Lista datos_central, llenada con los datos del archivo Excel.
        """
        self.xls = pd.ExcelFile(self.ruta)
        datos_central = []

        for nombre_hoja in self.xls.sheet_names:
            df = pd.read_excel(self.ruta, sheet_name=nombre_hoja)
            df = df.applymap(
                lambda x: x.replace("'", " ' ") if isinstance(x, str) else x
            )
            df.fillna("", inplace=True)

            if "maestro" in str(nombre_hoja).lower():
                nombre_tabla = f"{self.indice}_masterval_ctrls"
            elif "ingestiones" in str(nombre_hoja).lower():
                nombre_tabla = f"{self.indice}_ingestion_params"
            elif "monitoreo" in str(nombre_hoja).lower():
                nombre_tabla = f"{self.indice}_monitor_params"
            else:
                nombre_tabla = (
                    f'{self.indice}_{str(nombre_hoja).lower().replace(" ","_")}_params'
                )

            json_tab = str(df.to_dict(orient="list")).replace('"', "'")
            if (len(json_tab) / self.strlimit) > 1:
                for i in range(int(math.ceil(len(json_tab) / self.strlimit))):
                    if (i + 1) < int(math.ceil(len(json_tab) / self.strlimit)):
                        aux_json_tab = json_tab[
                            i * self.strlimit : (i + 1) * self.strlimit
                        ]
                    else:
                        aux_json_tab = json_tab[i * self.strlimit : len(json_tab)]
                    datos_central.append(
                        {
                            "nombre": nombre_hoja,
                            "json_tabla": aux_json_tab,
                            "tabla": str(nombre_tabla) + "_p" + str(i + 1),
                        }
                    )
            else:
                datos_central.append(
                    {
                        "nombre": nombre_hoja,
                        "json_tabla": json_tab,
                        "tabla": nombre_tabla,
                    }
                )

        return datos_central
