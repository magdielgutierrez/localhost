from helper.helper import Logger
from pathlib import Path
import getpass
import tempfile
import json
import os
import socket
import re
import requests
from sparky_bc.sparky import Sparky
from sparky_bc.manage_cred_livy import ManageCred
import getpass as gp
from datetime import datetime
import time
import pandas as pd
import base64

"""
The `SparkyCDP` class is a subclass of the `Sparky`
class and provides methods for interacting with Spark and HDFS.
It allows for submitting Spark jobs,
uploading and downloading files to/from HDFS,
and creating tables in the LZ (Landing Zone) using various file formats.

Example Usage:
```python
# Create an instance of LivySparky
sparky = SparkyCDP(username="user", dsn="dsn", cdp_endpoint="cdp_endpoint")

# Submit a Spark job
sparky.submit("script.py", spark_options="--executor-memory 4g")

# Upload a file to HDFS
sparky.subir_archivo("local_file.txt", "/user/hdfs_file.txt")

# Download a file from HDFS
sparky.bajar_archivo("/user/hdfs_file.txt", "local_file.txt")

# Create a table in the LZ from a CSV file
sparky.subir_csv("data.csv", "my_table")

# Execute SQL queries from a file
sparky.ejecutar_archivo_sql("queries.sql")
```

Main functionalities:
- Submit Spark jobs using Livy
- Upload and download files to/from HDFS
- Create tables in the LZ using various file formats
- Execute SQL queries from a file

Methods:
- `submit(archivo, spark_options="", application_arguments="", compress="infer")`: Submits a Spark job to Livy.
- `subir_archivo(path_local, path_remote, compress="infer")`: Uploads a file to HDFS.
- `bajar_archivo(path_remote, path_local)`: Downloads a file from HDFS.
- `subir_csv(path, nombre_tabla, zona="proceso", spark_options="", compress="infer", modo="error", particion=None, **kwargs)`: Creates a table in the LZ from a CSV file.
- `subir_parquet(path, nombre_tabla, zona="proceso", spark_options="", compress="infer", modo="error", particion=None, **kwargs)`: Creates a table in the LZ from a Parquet file.
- `subir_df(df, nombre_tabla, zona="proceso", spark_options="", compress="infer", modo="error", particion=None, parquet=False, **kwargs)`: Creates a table in the LZ from a pandas DataFrame.
- `ejecutar_archivo_sql(path, params=None, max_tries=2, spark_options="")`: Executes SQL queries from a file.

Fields:
- `livy_url`: The URL of the Livy server.
- `livy_user`: The username for Livy authentication.
- `livy_pwd`: The password for Livy authentication.
- `web_hdfs`: The URL of the WebHDFS server.
- `logger`: An instance of the `Logger` class for logging.
- `max_tries`: The maximum number of tries for executing a command.
- `client`: An instance of the `Client` class for interacting with HDFS.
"""


class SparkyCDP(Sparky):
    """
    Clase que ofrece un Api de alto nivel para interactuar con Spark Livy

    Atributes
    ---------
    username : str
        Nombre de usuario con el que se hará la conexion

    dsn: str
        Nombre del dsn para conectarse con Impala

    cdp_endpoint: str
        URL Endpoint CDP sobre el cual correrá la rutina

    ask_pwd : bool, Opcional, Default: `True`
        Si es `True` y no se ha proporcionado una contraseña en el parametro `password`
        se pedira la contraseña

    show_outp : bool, Opcional, Default: `False`
        Si es `True` mostrará en tiempo real la salida del comando ejecutado

    logger : Logger, Opcional
        Objeto encargado de administrar y mostrar el avance del plan de ejecucion, si este objeto
        es administrado, los parámetros `log_level` y `log_path` serán ignorados

    max_tries : int, Opcional, Default: 3
        Máxima cantidad de intentos que se realizará para
        ejecutar la tarea solicitada
    """
    def __init__(
        self,
        username,
        dsn,
        cdp_endpoint,
        ask_pwd=True,
        password=None,
        show_outp=False,
        logger=None,
        max_tries=3,
        **kwargs
    ):

        if issubclass(type(logger), Logger):
            self.logger = logger
        elif issubclass(type(logger), dict):
            logger["nombre"] = logger.get("nombre", "cdp spark")
            self.logger = Logger(**logger)
        elif logger is None:
            self.logger = Logger("cdp spark")
        else:
            raise TypeError("Parametro logger debe ser de tipo Logger o dict")

        username = username.lower()
        self.username = username
        self.hostname = socket.gethostname()
        self.ask_pwd = ask_pwd
        self.show_outp = show_outp
        self.dsn = dsn
        self.max_tries = max_tries
        self.nombre = self._validate_name(self.logger.nombre)

        self._livy_url = cdp_endpoint.strip("/") + "/livy/"
        self._web_hdfs = cdp_endpoint
        self.remote = True

        manager = ManageCred()

        self._session = manager.create_session(manager.encrypt(username, password))
        self._client = manager.create_hdfs_client(self._web_hdfs, manager.encrypt(username, password))

        if self.dsn is None or not isinstance(self.dsn, str) or len(self.dsn) == 0:
            raise Exception("Debe especificar el dsn configurado en su maquina")

        if self._livy_url is None or not isinstance(self._livy_url, str) or len(self._livy_url) == 0:
            raise Exception("Debe especificar el endpoint de livy")

        if self._web_hdfs is None or not isinstance(self._web_hdfs, str) or len(self._web_hdfs) == 0:
            raise Exception("Debe especificar el endpoint de webhdfs")

        self.kwargs = kwargs
        self.helper = self._instanciar_helper(self.dsn, logger=self.logger)

        if self.ask_pwd and not password:
            password = gp.getpass("{} password: ".format(self.username))

        # Crear directorios para guardar los archivos
        self.folder_hdfs = "/user/{}/sparky_bc/".format(self.username)
        try:
            self._client.delete("{}".format(self.folder_hdfs), recursive=True)
            self._client.makedirs("{}".format(self.folder_hdfs))
        except Exception as e:
            self.logger.log.exception(e)
            raise

    def _subir_archivos(
        self, task_id, paths_local, paths_remote, end="\n", compress=False
    ):
        """
        Método privado usado para subir varios archivos al servidor

        Parameters
        ----------
        task_id : int
            id de la tarea dentro del logger de Sparky

        paths_local : list[str]
            Rutas o nombres de los archivo que se desean subir al servidor

        paths_remote : list[str]
            Las rutas de destino en el servidor. Nota: Los nombres de los archivos
            debe quedar en la ruta. Especificar solo la carpeta producirá un error.

        compress : bool, Opcional, Default: `infer`
            Determina si se va a comprimir el archivo para una subida al servidor mas rápida o no, por defecto se comprime
            archivos con un tamaño superior a 1 megabyte
        """
        paths_local = list(map(str, paths_local))
        sizes = [os.stat(str(x)).st_size for x in paths_local]

        self.logger._reportar(task_id, "carga {:4.0%}".format(0))

        # Asume que tienes una clase Callback para el registro de eventos
        c = self.Callback(self.logger, task_id, sizes, 0)

        def progress(file, actual):
            total = os.stat(file).st_size
            if actual >= 0: c.callback(actual, total)
            else: c.callback(total, total)

        for path_local, path_remote, size in zip(paths_local, paths_remote, sizes):
            try:
                if os.path.exists(path_local):
                    try:
                        # upload the file
                        self._client.upload(path_remote, path_local, overwrite=True, progress=progress)
                        c.refresh_done(size, c.actual + 1)

                        #self.logger.info(f"Archivo {path_local} subido exitosamente a {path_remote}")

                    except Exception as e:
                        self.logger.error("Fallo al subir el archivo: {}".format(e))
                else:
                    self.logger.error("El archivo {} no existe".format(path_local))

            except Exception as e:
                self.logger.error("Error al subir el archivo {}: {}".format(path_local, e))


    def _bajar_archivo(self, task_id, path_remote, path_local ):
        """
        Método privado usado para bajar archivos del servidor mediante una
        conexión SFTP

        Parameters
        ----------
        task_id : int
            id de la tarea dentro del logger de Sparky

        paths_remote : str
            Rutas o nombres del archivo de origen en el servidor

        paths_local : str
            Rutas o nombres del archivo que se guardara en local

        """
        try:
            # Usar el método status para verificar la existencia del
            # archivo

            file_info = self._client.status(path_remote)
            size = file_info['length']

            c = self.Callback(self.logger, task_id, [size], 0)

            def progress(file, actual):
                total = size
                if actual >= 0: c.callback(actual, total)
                else: c.callback(total, total)

            if file_info["type"] != "FILE":
                self.logger.error("{} no es un archivo.".format(path_remote))
                return

            self.logger._reportar(task_id, "descarga {:4.0%}".format(0))

            # Usar el método download del cliente HDFS para descargar el
            # archivo
            self._client.download(path_remote, path_local, overwrite=True, progress=progress)
        except Exception as e:
            self.logger.error("Error al descargar el archivo: {}".format(e))



    def _submit(self, task_id, archivo, spark_options="", application_arguments="", compress=""):
        """
        Método privado usado para ejecutar una aplicacion de Spark ya sea de `Scala` (archivo `.jar`)
        o ya sea de `Python` (archivo `.py`)

        `NOTA`: Si desea que Sparky le notifique algo en particular durante la ejecucion incluya dentro de su código
        algo que imprima el siguiente comando:

        `info_orquestador: (su_notificacion)`

        reemplazando la parte `su_notificacion` por la palabra que Sparky le mostrará.

        Ejm: Si quiere que cuando su script de python vaya a empezar a leer un csv Sparky le informe que esta leyendo, antes de
        iniciar la lectura inserte el comando: `print('info_orquestador: (leyendo)')`

        Parameters
        ----------
        task_id : int
            id de la tarea dentro del logger de Sparky

        archivo : str
            Ruta o nombre del archivo el cual se desea que spark ejecute

        spark_options : str Opcional
            String con las opciones para levantar Spark Ejm:
            `"--driver-memory 10G --num-executors 10 --executor-memory 20G --conf spark.driver.maxResultSize=3000m"`

        application_arguments : str Opcional
            String con los argumentos que la aplicacion pudiera necesitar, su uso es igual al parametro `spark_options`

        compress : bool, Opcional, Default: `infer`
            Determina si se va a comprimir el archivo para una subida al servidor mas rápida o no, por defecto se comprime
            archivos con un tamaño superior a 1 megabyte
        """

        folder_hdfs = self.folder_hdfs

        headers = {"Content-Type": "application/json"}

        conf_keys = {'class':'className',
                     'py-files':'pyFiles',
                     'driver-memory':'driverMemory',
                     'driver-cores':'driverCores',
                     'executor-memory':'executorMemory',
                     'executor-cores':'executorCores',
                     'num-executors':'numExecutors'}

        int_params = ['driver-cores', 'executor-cores', 'num-executors']


        hdfs_file = folder_hdfs + "/" + Path(archivo).name
        self._subir_archivo(task_id, archivo, folder_hdfs, compress=compress)

        # Convert spark_options and application_arguments into the format that
        # Livy expects

        username = self.username
        spark_conf = {"file": hdfs_file,
                      "proxyUser": username,
                      "name":"sparky_{}_{}".format(username,datetime.now().strftime("%Y%m%d%H%M%S"))
                      }

        for option in spark_options.split("--"):
            option = option.strip()
            if option:
                key, value = option.split(" ", 1)

                if key == "conf":
                    conf_dict = spark_conf.get(key,{})
                    conf_dict[value.split('=')[0].strip()] = value.split('=')[1].strip()
                    spark_conf["conf"] = conf_dict
                else:
                    spark_conf[conf_keys.get(key,key)] = value if key not in int_params else int(value)

        encode = False
        if application_arguments != "" and '\\"' in application_arguments:
            application_arguments = json.loads(application_arguments.replace('\\"', '\\|').replace('"',"").replace('\\|', '"'))
        else:
            application_arguments = json.loads(application_arguments)

        encode = application_arguments.get("encode",False)
        application_arguments = json.dumps(application_arguments)

        if encode: application_arguments = base64.b64encode(application_arguments.encode("utf-8")).decode()

        spark_conf["args"] = [application_arguments, self.folder_hdfs, int(encode)]

        self.logger._reportar(
            task_id, estado="SPARK... (Submitting to Livy)".format()
        )

        response = requests.post(
            self._livy_url + "batches",
            headers=headers,
            data=json.dumps(spark_conf),
            auth=self._session.auth
        )

        if response.status_code != 201:
            raise ValueError("Failed to create batch")
        batch_id = response.json()["id"]

        self.logger._reportar(
            task_id, estado="SPARK... (Submitted to Livy, ID: {})".format(batch_id)
        )

        # Monitorear el estado del lote (batch)
        status = None
        while status not in ["success", "dead", "killed"]:
            try:
                response = requests.get(
                    self._livy_url + "/batches/" + str(batch_id), auth=self._session.auth
                )
                if response.status_code != 200:
                    response = requests.get(
                        self._livy_url + "/batches/" + str(batch_id) + "/log", auth=self._session.auth
                    )
                    log = response.text
                    self.logger.error("{}".format(log))
                    raise ValueError("Failed to get batch status")

                status = response.json()["state"]
                self.logger._reportar(task_id, estado="SPARK... ({}, ID: {})".format(status, batch_id))
                time.sleep(3)
                #self.logger.info("Batch status:{}".format(status))
            except:
                time.sleep(1)

        # Obtener el resultado o registro del lote (batch) si es exitoso
        if status != "success":
            log = ""
            try:
                with tempfile.TemporaryDirectory() as tempfolder:
                    file = "{}/sparky.log".format(tempfolder)
                    self._client.download(self.folder_hdfs + "/sparky.log", file, overwrite=True)

                    with open(file, "r") as file:
                        log = file.read()
                        self.logger.error(log)
            except:
                raise ValueError("Batch execution failed, error getting log")
            finally:
                raise ValueError("Batch execution failed. {}".format(log))

        try:
            # Limpiar script remoto
            self._client.delete(hdfs_file)
            self._client.delete(self.folder_hdfs + "/sparky.log")
        except:
            pass

    def _subir_csv(
        self,
        task_id,
        path,
        nombre_tabla,
        zona="proceso",
        end="\n",
        spark_options="",
        compress="infer",
        modo="error",
        particion=None,
        **kwargs
    ):
        """
        Método util para montar en la LZ una tabla que se encuentre en un archivo `CSV`, Se le pueden pasar los parametros
        usados por Spark para configurar la subida, disponibles en:
        https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=read%20csv#pyspark.sql.DataFrameReader.csv

        Parameters
        ----------
        task_id : int
            id de la tarea dentro del logger de Sparky

        path : str
            Nombre o ruta del archivo `CSV` a subir

        nombre_tabla : str
            Nombre de la tabla de como quedará en la LZ. Se puede especificar la zona en la que será guardado, si no se especifica
            será guardada en la zona del parámetro `zona`. `NOTA`: Si la tabla existe será sobrescrita.
            usela con cuidado.

        zona : str, Opcional, Default: `proceso`
            Zona en la que será guardada la base si no se especifica dentro del parametro `nombre_tabla`

        spark_options : str Opcional
            String con las opciones para levantar Spark Ejm:
            `"--driver-memory 10G --num-executors 10 --executor-memory 20G --conf spark.driver.maxResultSize=3000m"`

        compress : bool, Opcional, Default: `infer`
            Determina si se va a comprimir el archivo para una subida al servidor mas rápida o no, por defecto se comprime
            archivos con un tamaño superior a 1 megabyte

        modo : str Opcional
            Especifica el comportameinto de la escritura de la tabla en caso de que la tabla ya exista
                `append`: Inserta los registros a la tabla existente (La tabla existente y la nueva deben tener el mismo esquema)
                `overwrite`: Sobrescribe la tabla
                `ignore`: Ignora la escritura de la tabla en caso de existir  Silently ignore this operation if data already exists.
                `error` or `errorifexists` (valor por defecto): Levanta una excepción en caso de que la tabla ya exista

        particion : str or list(str) Opcional
            Nombre o nombres de columnas que se usarán para particionar la tabla

        """

        df = pd.read_csv(path, **kwargs)
        self._subir_df(task_id=task_id, df=df, nombre_tabla=nombre_tabla, zona=zona, spark_options=spark_options, compress=compress, modo=modo,
                    particion=particion, parquet=True, **kwargs)

    def _subir_parquet(
        self,
        task_id,
        path,
        nombre_tabla,
        zona="proceso",
        end="\n",
        spark_options="",
        compress="infer",
        modo="error",
        particion=None,
        **kwargs
    ):
        """
        Método privado para montar en la LZ una tabla desde un archivo de parquet

        Parameters
        ----------
        task_id : int
            id de la tarea dentro del logger de Sparky

        path : str
            Nombre o ruta del archivo `parquet` a subir

        nombre_tabla : str
            Nombre de la tabla de como quedará en la LZ. Se puede especificar la zona en la que será guardado, si no se especifica
            será guardada en la zona del parámetro `zona`. `NOTA`: Si la tabla existe será sobrescrita.
            usela con cuidado.

        zona : str, Opcional, Default: `proceso`
            Zona en la que será guardada la base si no se especifica dentro del parametro `nombre_tabla`

        spark_options : str Opcional
            String con las opciones para levantar Spark Ejm:
            `"--driver-memory 10G --num-executors 10 --executor-memory 20G --conf spark.driver.maxResultSize=3000m"`

        compress : bool, Opcional, Default: `infer`
            Determina si se va a comprimir el archivo para una subida al servidor mas rápida o no, por defecto se comprime
            archivos con un tamaño superior a 1 megabyte

        modo : str Opcional
            Especifica el comportameinto de la escritura de la tabla en caso de que la tabla ya exista
                `append`: Inserta los registros a la tabla existente (La tabla existente y la nueva deben tener el mismo esquema)
                `overwrite`: Sobrescribe la tabla
                `ignore`: Ignora la escritura de la tabla en caso de existir  Silently ignore this operation if data already exists.
                `error` or `errorifexists` (valor por defecto): Levanta una excepción en caso de que la tabla ya exista

        particion : str or list(str) Opcional
            Nombre o nombres de columnas que se usarán para particionar la tabla

        """

        # Configuracion para spark
        path = Path(
            path
        )  # Se hace para luego convertirla a ruta absoluta si es necesario
        hdfs_path = "{}/{}".format(self.folder_hdfs, path.name)
        config = {
            "ruta_origen": hdfs_path,
            "nombre_tabla": nombre_tabla,
            "zona": zona,
            "tipo": "parquet",
            "mode": modo,
            "partitionBy": particion,
            "kwargs": kwargs,
            "encode": True
        }

        params = json.dumps(config)

        try:
            # Determinar las rutas a los diferentes archivos
            self.logger._reportar(task_id, estado="subir HDFS")
            file_path = "{}/{}".format(
                self.folder_hdfs, path.name
            )  # Ruta del parquet en el servidor

            if path.is_dir():
                # Se suben todos los archivos de la carpeta
                paths_local = list(path.iterdir())
                paths_remote = [
                    "{}/{}".format(file_path, x.name) for x in paths_local
                ]
                self._subir_archivos(
                    task_id, paths_local, paths_remote, end="", compress=compress
                )
            else:
                # Se sube el archivo Parquet
                paths_local = [path.absolute()]
                paths_remote = [file_path]
                paths_remote = [f.replace(" ", "%20") for f in paths_remote] # Es necesario reemplazar los espacios para subir al HDFS

                self._subir_archivos(
                    task_id, paths_local, paths_remote, end="", compress=compress
                )

            application_arguments = '"' + params.replace('"', '\\"') + '"'
            self._submit(
                task_id,
                '{}/sparky_subir_tabla_livy.py'.format(self.obtener_ruta()),  # Ruta local del archivo,
                spark_options,
                application_arguments
            )

            self.logger._reportar(task_id, estado="invalidate")
            cursor = self.helper.obtener_cursor()
            tabla_destino = nombre_tabla if re.search(r'\.', nombre_tabla) else zona + '.' + nombre_tabla
            self.helper._ejecutar_consulta(cursor, task_id, 'set SYNC_DDL=1', '')
            self.helper._ejecutar_consulta(cursor, task_id, 'invalidate metadata ' + tabla_destino, '')
            self.helper._ejecutar_consulta(cursor, task_id, 'refresh ' + tabla_destino, '')
            self.helper._ejecutar_consulta(cursor, task_id, 'set SYNC_DDL=0', '')
        finally:
            try:
                # Eliminar los archivos subidos (no dejar basura)
                self._client.delete(file_path, recursive=True)
            except:
                self.logger.warning("No se lograron limpiar los archivos del servidor")


    def _subir_df(
        self,
        task_id,
        df,
        nombre_tabla,
        zona="proceso",
        end="\n",
        spark_options="",
        compress="infer",
        modo="error",
        particion=None,
        parquet=False,
        **kwargs
    ):
        """
        Método util para montar en la LZ una tabla que se encuentre en un DataFrame de pandas, Se le pueden pasar los parametros
        usados por Spark para configurar la subida, disponibles en:
        https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=read%20csv#pyspark.sql.DataFrameReader.csv


        Parameters
        ----------
        path : str
            Nombre o ruta del archivo `CSV` a subir

        nombre_tabla : str
            Nombre de la tabla de como quedará en la LZ. Se puede especificar la zona en la que será guardado, si no se especifica
            será guardada en la zona del parámetro `zona`. `NOTA`: Si la tabla existe será sobrescrita.
            usela con cuidado.

        zona : str, Opcional, Default: `proceso`
            Zona en la que será guardada la base si no se especifica dentro del parametro `nombre_tabla`

        end : str
            Parametro usado para el manejo de la presentacion del avance en consola

        compress : bool, Opcional, Default: `infer`
            Determina si se va a comprimir el archivo para una subida al servidor mas rápida o no, por defecto se comprime
            archivos con un tamaño superior a 1 megabyte

        modo : str Opcional
            Especifica el comportameinto de la escritura de la tabla en caso de que la tabla ya exista
                `append`: Inserta los registros a la tabla existente (La tabla existente y la nueva deben tener el mismo esquema)
                `overwrite`: Sobrescribe la tabla
                `ignore`: Ignora la escritura de la tabla en caso de existir  Silently ignore this operation if data already exists.
                `error` or `errorifexists` (valor por defecto): Levanta una excepción en caso de que la tabla ya exista

        particion : str or list(str) Opcional
            Nombre o nombres de columnas que se usarán para particionar la tabla

        """
        with tempfile.TemporaryDirectory() as tempfolder:
            self.logger._reportar(task_id, estado="convirtiendo")
            file = "{}/tabla_{}.parq".format(tempfolder, self.nombre)
            df[df.select_dtypes(include=["object"]).columns] = df.select_dtypes(
                include=["object"]
            ).astype(
                str
            )  # conversión de objet a string
            df = df.replace(
                "nan", None
            )  # remplazar nan a None parque en la LZ quede como Null
            df.to_parquet(file)

            self._subir_parquet(
                task_id,
                file,
                nombre_tabla,
                zona,
                end,
                spark_options,
                compress,
                modo,
                particion,
                **kwargs
            )

    def _subir_excel(
        self,
        task_id,
        path,
        nombre_tabla,
        zona="proceso",
        end="\n",
        spark_options="",
        compress="infer",
        modo="error",
        particion=None,
        **kwargs
    ):
        """
        Método util para montar en la LZ una tabla que se encuentre en un archivo de excel, Se le pueden pasar los parametros
        usados por pandas para leer un archivo de excel, disponibles en:
        https://pandas.pydata.org/pandas-docs/version/0.20.3/generated/pandas.read_excel.html

        Parameters
        ----------
        task_id : int
            id de la tarea dentro del logger de Sparky

        path : str
            Nombre o ruta del archivo `CSV` a subir

        nombre_tabla : str
            Nombre de la tabla de como quedará en la LZ. Se puede especificar la zona en la que será guardado, si no se especifica
            será guardada en la zona del parámetro `zona`. `NOTA`: Si la tabla existe será sobrescrita.
            usela con cuidado.

        zona : str, Opcional, Default: `proceso`
            Zona en la que será guardada la base si no se especifica dentro del parametro `nombre_tabla`

        spark_options : str Opcional
            String con las opciones para levantar Spark Ejm:
            `"--driver-memory 10G --num-executors 10 --executor-memory 20G --conf spark.driver.maxResultSize=3000m"`

        compress : bool, Opcional, Default: `infer`
            Determina si se va a comprimir el archivo para una subida al servidor mas rápida o no, por defecto se comprime
            archivos con un tamaño superior a 1 megabyte

        modo : str Opcional
            Especifica el comportameinto de la escritura de la tabla en caso de que la tabla ya exista
                `append`: Inserta los registros a la tabla existente (La tabla existente y la nueva deben tener el mismo esquema)
                `overwrite`: Sobrescribe la tabla
                `ignore`: Ignora la escritura de la tabla en caso de existir  Silently ignore this operation if data already exists.
                `error` or `errorifexists` (valor por defecto): Levanta una excepción en caso de que la tabla ya exista

        particion : str or list(str) Opcional
            Nombre o nombres de columnas que se usarán para particionar la tabla

        """

        df = pd.read_excel(path, **kwargs)
        self._subir_df(task_id=task_id, df=df, nombre_tabla=nombre_tabla, zona=zona, spark_options=spark_options, compress=compress, modo=modo,
                    particion=particion, parquet=True, **kwargs)


    def _ejecutar_consultas_sql(self, task_id, queries, max_tries=1, spark_options=""):
        """
        Método semi-privado para ejecutar una lista de consultas SQL HIVE
        """
        conf = {"queries": queries, "max_tries": max_tries, "encode":True}
        config = json.dumps(conf)

        self._submit(
            task_id,
            "{}/sparky_ejecutar_sql_livy.py".format(
                self.obtener_ruta()
            ),  # Ruta local del archivo,
            spark_options,
            config,
        )

    def _ejecutar_calificador(
        self,
        task_id,
        modelo,
        tabla_origen,
        tabla_destino,
        zona="proceso",
        variables=[],
        spark_options="",
        modo="error",
        particion=None,
    ):
        """
        Método privado para ejecutar un modelo de Spark. Para mayor informacion ver la documentacion del metodo
        público `Sparky.ejecutar_calificador`

        Parameters
        ----------
        task_id : int
            id de la tarea dentro del logger de Sparky

        modelo : str
            Ruta o direccion del HDFS en la cual se encuentra guardado el modelo de `Spark`

        tabla_origen : str
            Nombre de la tabla insumo para el modelo (debe tener la zona), ej: `proceso.tabla_para_calificar`

        tabla_destino : str
            Nombre de la tabla en la cual se guardarán los resultados, si la tabla existe, se sobrescribirá
            (debe tener la zona) ej: `proceso.tabla_de_resultados`

        zona : str
            Zona por defecto que se usara en caso de no ser especificado en las tablas de origen o destino

        variables : list[str], Opcional
            Lista de las variables que se quisieran llevar a la LZ, por defecto se llevan todas las variables a la
            LZ, se pueden especificar que se extraigan valores numericos de columnas que sean vectores de la siguiente
            manera: `col_name[0]` lo cual te extraera el primer valor del vector de la columna `col_name` tambien se
            pueden especificar nombres para las variables de la misma manera que en sql. Ejm:
            `["col1","col2","col3[0]","col3[1] as columna4","col3[2] col5"]` lo cual dará como resultado una tabla
            en la LZ con las siguientes variables `col1, col2, col3_0, columna4, col5`

        spark_options : str Opcional
            String con las opciones para levantar Spark Ejm:
            `"--driver-memory 10G --num-executors 10 --executor-memory 20G --conf spark.driver.maxResultSize=3000m"`

        modo : str Opcional
            Especifica el comportameinto de la escritura de la tabla en caso de que la tabla ya exista
                `append`: Inserta los registros a la tabla existente (La tabla existente y la nueva deben tener el mismo esquema)
                `overwrite`: Sobrescribe la tabla
                `ignore`: Ignora la escritura de la tabla en caso de existir  Silently ignore this operation if data already exists.
                `error` or `errorifexists` (valor por defecto): Levanta una excepción en caso de que la tabla ya exista

        particion : str or list(str) Opcional
            Nombre o nombres de columnas que se usarán para particionar la tabla

        """
        self.logger._reportar(task_id, estado="cargando")

        # Revisar formato de las variables
        revisar_columnas = [
            re.match(r"(\w+)(\[(\w+)\])?((\s+as)?\s+(\w+))?$", x, re.IGNORECASE)
            for x in variables
        ]
        columnas_malas = [x for x, y in zip(variables, revisar_columnas) if not y]
        assert all(revisar_columnas), "Error en el formato de las columnas {}".format(
            columnas_malas
        )

        config = {
            "modelo": modelo,
            "tabla_orig": tabla_origen,
            "tabla_dest": tabla_destino,
            "zona": zona,
            "variables": variables,
            "mode": modo,
            "partitionBy": particion,
        }
        params = json.dumps(config)
        self._submit(
            task_id,
            "{}/sparky_calificacion_livy.py".format(
                self.obtener_ruta()
            ),  # Ruta local del archivo,
            spark_options,
            "'{}'".format(params),
        )
        self.logger._reportar(task_id, estado="invalidate")
        cursor = self.helper.obtener_cursor()
        nombre_tabla = (
            tabla_destino
            if re.search(r"\.", tabla_destino)
            else zona + "." + tabla_destino
        )
        self.helper._ejecutar_consulta(cursor, task_id, "set SYNC_DDL=1", "")
        self.helper._ejecutar_consulta(
            cursor, task_id, "invalidate metadata " + nombre_tabla, ""
        )
        self.helper._ejecutar_consulta(cursor, task_id, "refresh " + nombre_tabla, "")
        self.helper._ejecutar_consulta(cursor, task_id, "set SYNC_DDL=0", "")

