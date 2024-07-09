# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- Equipo Vicepresidencia de Ecosistemas
-----------------------------------------------------------------------------
-- Fecha Creación: 20240502
-- Última Fecha Modificación: 20240517
-- Autores: seblopez
-- Últimos Autores: seblopez
-- Descripción:   Modulo para respaldar los logs de ejecución de Orquestador2
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
"""

from datetime import datetime
from getpass import getuser
import os
import re
import socket
import base64


class SaveLogFile:
    """Respaldar el archivo de log de ejecución de Orquestador2 en la Landing Zone"""

    def __init__(self, active: bool = True):
        """Respaldar el archivo de log de ejecución de Orquestador2 en la Landing Zone

        Args:
            active (bool, optional): Activa o desactiva el respaldo de logs. Defaults to True.
        """
        self.active = active
        self.test_flag = False
        self.path = os.path.dirname(__file__)
        self.logger = None
        self.helper = None
        self.tabla = ""
        # Datos rutina
        self.informacion = {}
        # Formato de comparación de la tabla
        self.comparar = {
            "rutina": "STRING",
            "ejecutor": "STRING",
            "ip": "STRING",
            "fecha_inicio": "TIMESTAMP",
            "fecha_fin": "TIMESTAMP",
            "tiempo_minutos": "DOUBLE",
            "estado": "STRING",
            "error": "STRING",
            "log_name": "STRING",
            "log_file": "STRING",
            "year": "INT",
        }

    def _validar_activacion(self):
        """Método privado que valida si la clase está activa o no. Si no está activa, se desactiva el log de la rutina.

        Args:
            orquestador (object): Objeto de orquestador2
        """
        if self.active == False:
            self.logger.warning(
                "No se subirán logs de ejecución ya que 'Save_log_file' está desactivada"
            )
            return False
        else:
            return True

    def _validar_parametro_tbl_logs(self, orquestador: object):
        """Método privado que valida si el parámetro 'tbl_logs' está en la configuración global del orquestador.

        Args:
            orquestador (object): Objeto de orquestador2
        """
        if "tbl_logs" not in orquestador.globalConfig:
            self.logger.warning(
                "No se subieron logs ya que no está configurado el parámetro: 'tbl_logs'"
            )
            self.test_flag = True
            return False
        else:
            # Patrón de la expresión regular
            patron = r"(resultados|proceso)_[a-zA-Z]+(?:_[a-zA-Z]+)*\.[a-zA-Z_]*[a-zA-Z][a-zA-Z_]*$"
            self.tabla = orquestador.globalConfig["tbl_logs"]
            if not (self.tabla != "" and re.match(patron, self.tabla)):
                mensaje = f"No se subió logs ya que la tabla '{self.tabla}' no es una tabla determinada en 'proceso_' o 'resultados_'"
                self.logger.warning(mensaje)
                self.test_flag = True
                return False
            else:
                return True

    def _validar_carpeta_logs(self, orquestador: object):
        """Método privado que valida si la carpeta de logs existe en el orquestador.

        Args:
            orquestador (object): Objeto de orquestador2
        """
        if not os.path.exists(orquestador.log_path):
            self.logger.warning(
                "No se subieron logs ya que no existe directorio donde se almacenan los logs"
            )
            self.test_flag = True
            return False
        else:
            return True

    def _validar_existencia_tabla(self, tabla: str, max_tries: int = 3):
        """Método privado que valida si la tabla existe en la base de datos.

        Args:
            tabla (str): Tabla a la cual se le validará su existencia o se creará
            max_tries (int, optional): Cantidad de intentos. Defaults to 3.
        """
        # Seleccionar la base de datos
        self.helper.ejecutar_consulta(f"use {tabla.split('.')[0]};")
        # Traer tablas existentes en la base de datos
        lista_tablas = self.helper.obtener_dataframe("show tables;")
        # Validar existencia de la tabla
        if not tabla.split(".")[-1] in lista_tablas["name"].to_list():
            mensaje = f"No se encontró la tabla '{tabla}' para subir los logs. Se creará la tabla"
            self.logger.warning(mensaje)
            comparar_str = ", ".join([f"{key} {value}" for key, value in self.comparar.items() if key != "year"])
            sql_file = f"{self.path}/static/sql/create_save_logs_table.sql"
            parametros = {"tabla": tabla, "comparar_str": comparar_str}
            intento = 0
            while intento <= max_tries:
                intento += 1
                try:
                    self.helper.ejecutar_archivo(
                        sql_file,
                        params=parametros,
                    )
                    break
                except Exception as e:
                    mensaje = f"Falla el intento {intento} de {max_tries}"
                    self.logger.warning(mensaje)
                    self.logger.warning(e)
                    if intento == max_tries:
                        self.logger.warning("No se logró crear la tabla para subir los logs")
                        return False
                    else:
                        continue
        self.test_flag = True
        return True


    def _validar_estructura_tabla(self):
        """Método privado que valida si la estructura de la tabla es la adecuada para subir los logs.
        """
        # Traer formato de la tabla donde se cargará los logs
        formato = self.helper.obtener_dataframe(f"describe {self.tabla};")
        formato = formato.set_index("name")["type"].to_dict()
        # ¿Hay más columnas de las necesarias?
        try:
            # ¿la columna tiene el type_data apropiado?
            for key, value in formato.items():
                if self.comparar[key].lower() != str(value).lower():
                    necesarias = [f"{key} as {value}" for key, value in self.comparar.items()]
                    necesarias = ',\n'.join(necesarias)
                    mensaje = f"Estructura de la tabla debe ser:\n{necesarias}"
                    self.logger.warning(mensaje)
                    self.test_flag = True
                    return False
        except KeyError:
            # Columna a comparar no está en las columnas de la variable 'comparar'
            necesarias = [f"{key} as {value}" for key, value in self.comparar.items()]
            necesarias = '\n'.join(necesarias)
            mensaje = f"La tabla '{self.tabla}' no tiene las columnas apropiadas. Deben ser:\n{necesarias}"
            self.logger.warning(mensaje)
            self.test_flag = True
            return False
        # ¿Hay menos columnas de las necesarias?
        if not all(x in list(formato.keys()) for x in list(self.comparar.keys())):
            # Columna a comprar tiene menos columnas que la variable 'comparar'
            necesarias = ', '.join(list(self.comparar.keys()))
            dadas = ', '.join(list(formato.keys()))
            mensaje = f"La tabla '{self.tabla}' contiene menos columnas de las necesarias"
            self.logger.warning(mensaje)
            mensaje = f"Columnas necesarias: {necesarias}\nColumnas dadas: {dadas}"
            self.logger.warning(mensaje)
            self.test_flag = True
            return False
        # Determinar si existen particiones y que sea únicamente 'year'
        describe = self.helper.obtener_dataframe(f"describe formatted {self.tabla};")
        columnas = []
        for index, row in describe.iterrows():
            # ¿La tabla tiene particiones?
            if "# Partition Information" in row.iloc[0].strip():
                partitions = self.helper.obtener_dataframe(f"show partitions {self.tabla};")
                # Crar lista con columnas en orden hasta que encuentre la primera columna con '#' al principio, aquí termina la lista de columnas
                for columna in partitions.columns:
                    if columna.startswith("#"):
                        break
                    columnas.append(columna)
                break
        # Validar que la partición year esté en la tabla
        if len(columnas) == 1:
            if columnas[0] != "year":
                mensaje = f"La tabla '{self.tabla}' debe tener como partición única la columna 'year'"
                self.logger.warning(mensaje)
                mensaje = f"Particiones encontradas: {', '.join(columnas)}"
                self.logger.warning(mensaje)
                self.test_flag = True
                return False
        elif len(columnas) == 0:
            mensaje = f"La tabla '{self.tabla}' no tiene particiones. Debe tener como partición única la columna 'year'"
            self.logger.warning(mensaje)
            self.test_flag = True
            return False
        else:
            mensaje = f"La tabla '{self.tabla}' debe tener como partición única la columna 'year'"
            self.logger.warning(mensaje)
            mensaje = f"Particiones encontradas: {', '.join(columnas)}"
            self.logger.warning(mensaje)
            self.test_flag = True
            return False
        return True

    def _informacion_rutina(self, orquestador: object, start: datetime, excepcion: str = ""):
        """Método privado que obtiene la información de la rutina para subir a la LZ.

        Args:
            orquestador (object): Objeto de orquestador2
            start (datetime): fecha/hora inicio de la ejecución
            excepcion (str, optional): Si hay error, trae la Exception asociada. Defaults to None.
        """
        # Determinar la información de la ejecución de la rutina
        self.informacion["rutina"] = orquestador.packageName
        self.informacion["ejecutor"] = getuser()
        self.informacion["ip"] = socket.gethostbyname(socket.gethostname())
        self.informacion["fecha_inicio"] = start.strftime("%Y-%m-%d %H:%M:%S")
        self.informacion["fecha_fin"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.informacion["tiempo_minutos"] = round((datetime.now() - start).total_seconds() / 60, 2)
        self.informacion["estado"] = "Successful" if excepcion == "" else "Unsuccessful"
        self.informacion["error"] = str(excepcion).replace("\'", "\"")
        self.informacion["log_name"] = max(
            os.listdir(orquestador.log_path),
            key=lambda x: os.path.getmtime(os.path.join(orquestador.log_path, x)),
        )
        last_log_path = os.path.realpath(f"{orquestador.log_path}/{self.informacion['log_name']}")
        with open(last_log_path, mode="r") as file:
            log_read = file.read()
        self.informacion["log_file"] = base64.b64encode(log_read.encode("utf-8")).decode("utf-8")
        self.informacion["year"] = start.year

    def _upload_logs(self, max_tries: int = 3):
        """Método privado que sube los logs a la LZ.

        Args:
            max_tries (int, optional): Máximos intentos para subir los logs. Defaults to 3.
        """
        sql_file = f"{self.path}/static/sql/insert_save_logs.sql"
        # Unificar dos diccionarios
        self.informacion["tabla"] = self.tabla
        with open(sql_file, mode="r") as file:
            consultas = file.read()
        consultas = consultas.split(";")[:-1]
        intento = 0
        for consulta in consultas:
            while intento <= max_tries:
                intento += 1
                try:
                    self.helper.ejecutar_consulta(f"{consulta};", params=self.informacion)
                    break
                except Exception as e:
                    mensaje = f"Falla el intento {intento} de {max_tries}"
                    self.logger.warning(mensaje)
                    self.logger.warning(e)
                    if intento == max_tries:
                        self.logger.warning("No se logró subir los logs de ejecución")
                        self.test_flag = True
                        return
                    else:
                        continue
        self.test_flag = True

    def save_log_file(self, orquestador: object, start: datetime, excepcion: str = "", max_tries: int = 3):
        """Sube información a la Landing Zone sobre la ejecución de la rutina. El valor de la llave 'tbl_logs' en la
        configuración global debe estar apuntando a una tabla en zona de 'resultados_' o 'proceso_'.

        Args:
            orquestador (object): Objeto de orquestador2
            start (datetime): fecha/hora inicio de la ejecución
            excepcion (str, optional): Si hay error, trae la Exception asociada. Defaults to None.
            max_tries (int, optional): Máximos intentos para subir el log.
        """
        # Obtener la instancia de Log
        self.logger = orquestador.log
        # Obtener la instancia de Helper
        self.helper = orquestador.helper
        # Se utiliza para no llenar el script de ifs anidados y conservar buenas prácticas
        validator = None
        # Realizar validaciones
        validator = self._validar_activacion()
        if validator:
            validator = self._validar_parametro_tbl_logs(orquestador)
        if validator:
            validator = self._validar_carpeta_logs(orquestador)
        if validator:
            validator = self._validar_existencia_tabla(self.tabla, max_tries)
        if validator:
            validator = self._validar_estructura_tabla()
        if validator:
            self._informacion_rutina(orquestador, start, excepcion)
            self._upload_logs(max_tries)
