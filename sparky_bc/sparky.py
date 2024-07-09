from helper.helper import Helper, Logger
from .remote import Remote
from pathlib import Path
import getpass as gp
import pkg_resources
import subprocess
import platform
import tempfile
import tarfile
import json
import os
import re
import string
import random

class Sparky:
    """
    Clase que ofrece un Api de alto nivel para interactuar con Spark y en los servidores del Banco

    Atributes
    ---------
    username : str
        Nombre de usuario con el que se hará la conexion

    ask_pwd : bool, Opcional, Default: `True`
        Si es `True` y no se ha proporcionado una contraseña en el parametro `password`
        se pedira la contraseña

    remote : bool, Opcional, Default: `True`
        Determina si la ejecucion de Spark debe se hacerse de manera remota o local

    hostname : str, Opcional, Default: "sbmdeblze003.bancolombia.corp"
        Direccion del servidor al cual se desea conectarse si la conexion es remota

    port : int, Opcional, Default: 22
        Puerto al que se conectará al servidor

    show_outp : bool, Opcional, Default: `False`
        Si es `True` mostrará en tiempo real la salida del comando ejecutado

    logger : Logger, Opcional
        Objeto encargado de administrar y mostrar el avance del plan de ejecucion, si este objeto
        es administrado, los parámetros `log_level` y `log_path` serán ignorados

    max_tries : int, Opcional, Default: 3
        Máxima cantidad de intentos que se realizará para
        ejecutar la tarea solicitada
    """

    def __init__(self,
                 username,
                 dsn,
                 ask_pwd=True,
                 password=None,
                 remote='infer',
                 hostname="sbmdeblze003.bancolombia.corp",
                 port=22,
                 show_outp=False,
                 logger=None,
                 max_tries=3,
                 **kwargs
                 ):
        """
        Parameters
        ----------
        username : str
            Nombre de usuario con el que se va a conectar

        dsn : str
            Driver de conexion a impala configurado localmente por el usuario

        ask_pwd : bool, Opcional, Default: `True`
            Si es `True` y no se ha proporcionado una contraseña en el parametro `password`
            se pedira la contraseña

        password : str, Opcional
            Contraseña para conectarse, si no se proporciona y el parametro `ask_pwd`
            está en verdadero, sparky preguntará por ella

        remote : bool, Opcional, Default: `"infer"`
            Determina si la ejecucion de Spark debe se hacerse de manera remota o local
            por defecto el propio Sparky determina si debe ser remoto o local

        hostname : str, Opcional, Default: "sbmdeblze003.bancolombia.corp"
            Direccion del servidor al cual se desea conectarse si la conexion es remota

        port : int, Opcional, Default: 22
            Puerto al que se conectará al servidor

        show_outp : bool, Opcional, Default: False
            Si es `True` mostrará en tiempo real la salida del comando ejecutado

        logger : Logger or dict, Opcional
            Objeto encargado de administrar y mostrar el avance del plan de ejecucion.
            Si se pasa un diccionario, este debe contener los parametros para la creacion del
            logger. Ver la documentacion de Logger para mayor informacion

        kwargs : dict, Opcional
            Parametro para configuraciones adicionales
        """
        if issubclass(type(logger), Logger):
            self.logger = logger
        elif issubclass(type(logger), dict):
            logger['nombre'] = logger.get('nombre', 'remote spark')
            self.logger = Logger(**logger)
        elif logger is None:
            self.logger = Logger('remote spark')
        else:
            raise TypeError('Parametro logger debe ser de tipo Logger o dict')

        username = username.lower()
        self.username = username
        self.hostname = hostname
        self.port = port
        self.ask_pwd = ask_pwd
        self.show_outp = show_outp
        self.dsn = dsn
        self.max_tries = max_tries
        self.nombre = self._validate_name(self.logger.nombre)

        if remote == 'infer':
            try:
                test = subprocess.Popen(
                    'spark-submit --version',
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    shell=True
                ).wait()
            except:
                test = 1

            self.remote = test != 0 or platform.system() != 'Linux'
        else:
            assert remote in [True, False], 'parametro remote debe ser True, False, o "infer"'
            self.remote = remote

        # self.helper = self._instanciar_helper(helper=helper, logger=self.logger, username=username, password=password)
        if self.dsn is None or not isinstance(self.dsn, str) or len(self.dsn) == 0:
            raise Exception("Debe especificar el dsn configurado en su maquina")

        self.kwargs = kwargs
        self.helper = self._instanciar_helper(self.dsn, logger=self.logger)

        if self.remote == True:
            if self.ask_pwd and not password:
                password = gp.getpass("{} password: ".format(self.username))
            import paramiko  # Asegurase primero que tenga instalado paramiko o si no que explote
            self.__remote = Remote(username, password)


        # Crea un string aleatorio para evitar que se sobreescriban los archivos
        random_str = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(10))

        # Crear directorios para guardar los archivos
        self.folder_server = "/home/{}/sparky_bc_{}/".format(self.username, random_str)
        self.folder_hdfs = "/user/{}/sparky_bc_{}/".format(self.username, random_str)


        # try:
        #     self.__ejecutar('mkdir -p "{}"'.format(self.folder_server), program='Bash', error_pattern='.+')
        #     self.__ejecutar('rm -rf {}/*'.format(self.folder_server), program='Bash', error_pattern='.+')
        #     self.__ejecutar('hdfs dfs -mkdir -p "{}"'.format(self.folder_hdfs), program='HDFS', error_pattern='.+')
        #     self.__ejecutar('hdfs dfs -rm -r -f "{}/*"'.format(self.folder_hdfs), program='HDFS', error_pattern='.+')
        # except Exception as e:
        #     self.logger.log.exception(e)
        #     raise

    def _delete_self_folders(self):
        """
        ----------
        Description
        ----------
        Método que elimina las carpetas temporales creadas por Sparky

        ----------
        Parameters
        ----------
        None

        ----------
        Returns
        ----------
        None

        ----------
        Raises
        ----------
        AssertionError
            Si no se puede eliminar la carpeta temporal
        """
        try:
            # Eliminar carpeta hdfs
            self.__ejecutar('hdfs dfs -rm -r "{}"'.format(self.folder_hdfs), program='HDFS', error_pattern='.+')

            # Eliminar carpeta server
            self.__ejecutar('rmdir "{}"'.format(self.folder_server), program='Bash', error_pattern='.+')
        except Exception as e:
            self.logger.log.exception(e)
            raise

    def _create_self_folders(self):
        """
        ----------
        Description
        ----------
        Método que crea las carpetas temporales para Sparky

        ----------
        Parameters
        ----------
        None

        ----------
        Returns
        ----------
        None

        ----------
        Raises
        ----------
        AssertionError
            Si no se puede crear la carpeta temporal

        """
        try:
            # Crear carpeta server
            self.__ejecutar('mkdir -p "{}"'.format(self.folder_server), program='Bash', error_pattern='.+')

            # Crear carpeta hdfs
            self.__ejecutar('hdfs dfs -mkdir -p "{}"'.format(self.folder_hdfs), program='HDFS', error_pattern='.+')
        except Exception as e:
            self.logger.log.exception(e)
            raise


    def _validate_name(self, name):
        import unicodedata
        import re

        final_name = re.sub('\s+', ' ', name.strip()).lower().replace(" ", "_").replace("-", "_")
        final_name = unicodedata.normalize('NFKD', final_name).encode('ascii', 'ignore').decode('utf-8', 'ignore')

        if final_name in ['orquestador', 'remote_spark', 'impala_helper', '']:
            final_name = ''.join(random.choice(string.ascii_lowercase) for _ in range(5))

        return final_name

    def _instanciar_helper(self, dsn, logger=None):
        hlpr = Helper(logger=logger, dsn=dsn, **self.kwargs)
        return hlpr

    class Callback:
        def __init__(self, logger, task_id, files=None, actual=None):
            self.logger = logger
            self.task_id = task_id
            self.perc = 0
            self.total = sum(files) if files else None
            self.files = files
            self.actual = actual
            self.done = 0

        def callback(self, actual, total):
            perc = round(actual / total, 2) if self.total is None else round(
                ((actual / total * self.files[self.actual]) + self.done) / self.total, 2)
            if perc != self.perc:
                self.perc = perc
                self.logger._reportar(self.task_id, 'carga {:4.0%}'.format(perc))

        def porcentaje(self):
            return round(self.done / self.total, 2) if self.total is not None else 0

        def refresh_done(self, done, actual=None):
            self.done += done
            self.actual = actual

    def _bajar_archivo(self, task_id, path_remote, path_local ):
        """
        Método privado usado para bajar archivos del servidor mediante una
        conexión SFTP

        Parameters
        ----------
        task_id : int
            id de la tarea dentro del logger de Sparky

        paths_remote : str
            Rutas o nombres de el archivo de origen en el servidor

        paths_local : str
            Rutas o nombres de el archivo que se guardara en local

        """

        if self.remote:
            self.logger._reportar(task_id, 'carga {:4.0%}'.format(0))
            with self.__remote.connect(hostname=self.hostname, port=self.port) as client:
                with client.open_sftp() as sftp:
                    sftp.get(
                        path_remote,
                        path_local
                    )
        else:
            self.logger.warning('Ejecutandose en modo local, no hay de donde bajar el archivo')

    def _subir_archivos(self, task_id, paths_local, paths_remote, end='\n', compress='infer'):
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
        # Clase que ayudará en la muestra del avance de la carga del archivo
        if self.remote:
            paths_local = list(map(str, paths_local))
            sizes = [os.stat(str(x)).st_size for x in paths_local]
            total_size = sum(sizes)
            # comprimir
            comprimir = compress if type(compress) is bool else total_size >= pow(2,
                                                                                  20)  # comprimir si es mayor a 1 mega
            self.logger._reportar(task_id, 'carga {:4.0%}'.format(0))
            c = self.Callback(self.logger, task_id, sizes, 0)
            with self.__remote.connect(hostname=self.hostname, port=self.port) as client:
                with client.open_sftp() as sftp:

                    # Para no tener problemas con archivos grandes
                    channel = sftp.get_channel()
                    channel.lock.acquire()
                    size = channel.out_window_size + total_size
                    channel.out_window_size = size
                    channel.in_window_size = size
                    channel.in_max_packet_size = size
                    # TODO: Se debe definir el parámetro out_max_packet_size
                    # channel.out_max_packet_size = size
                    channel.transport.packetizer.REKEY_BYTES = pow(2, 40)
                    channel.transport.packetizer.REKEY_PACKETS = pow(2, 40)
                    channel.out_buffer_cv.notifyAll()
                    channel.lock.release()

                    for path_local, path_remote, size in zip(paths_local, paths_remote, sizes):
                        if comprimir:
                            self.logger._reportar(task_id, 'comprim {:4.0%}'.format(c.porcentaje()))
                            tempfolder = tempfile.TemporaryDirectory()
                            ruta_archivo = '{}/{}.tar.gz'.format(tempfolder.name, Path(path_local).name)
                            ruta_remota = '{}.tar.gz'.format(path_remote)
                            with tarfile.open(ruta_archivo, 'w:gz') as archivo:
                                archivo.add(str(path_local), arcname=Path(path_local).name)

                        sftp.put(
                            str(path_local) if not comprimir else ruta_archivo,
                            path_remote if not comprimir else ruta_remota,
                            c.callback
                        )

                        c.refresh_done(size, c.actual + 1)

                        if comprimir:
                            self.logger._reportar(task_id, 'descomp {:4.0%}'.format(c.porcentaje()))
                            # Sacar la carpeta del archivo organizando que no pase nada raro en windows
                            carpeta = str(Path(path_remote).parent).replace('\\', '/')
                            self.__ejecutar(
                                'cd "{}"\ntar -zxf {}'.format(carpeta, ruta_remota),
                                program='Bash',
                                error_pattern='.+'
                            )
                            self.__ejecutar('rm {}'.format(ruta_remota), program='Bash', error_pattern='.+')
                            tempfolder.cleanup()
        else:
            self.logger.warning('Ejecutandose en modo local, no hay donde subir el(los) archivo')

    def _subir_archivo(self, task_id, path_local, path_remote, end='\n', compress='infer'):
        """
        Método privado usado para subir un archivo al servidor

        Parameters
        ----------
        task_id : int
            id de la tarea dentro del logger de Sparky

        path_local : str
            Ruta o nombre del archivo el cual se desea subir al servidor

        path_remote : str
            La ruta de destino en el servidor. Nota: El nombre del archivo
            debe quedar en la ruta. Especificar solo la carpeta producirá un error.

        compress : bool, Opcional, Default: `infer`
            Determina si se va a comprimir el archivo para una subida al servidor mas rápida o no, por defecto se comprime
            archivos con un tamaño superior a 1 megabyte
        """
        if self.remote:
            self._subir_archivos(task_id, [path_local], [path_remote], end, compress)
        else:
            self.logger.warning('Ejecutandose en modo local, no hay donde subir el archivo')

    def subir_archivo(self, path_local, path_remote, compress='infer'):
        """
        Método usado para subir un archivo al servidor si te encuentras en modo remoto, de lo contrario
        si te encuentras en modo local este método no hace nada

        Parameters
        ----------
        path_local : str
            Ruta o nombre del archivo el cual se desea subir al servidor

        path_remote : str
            La ruta de destino en el servidor. Nota: El nombre del archivo
            debe quedar en la ruta. Especificar solo la carpeta producirá un error.

        compress : bool, Opcional, Default: `infer`
            Determina si se va a comprimir el archivo para una subida al servidor mas rápida o no, por defecto se comprime
            archivos con un tamaño superior a 1 megabyte
        """
        p = Path(path_local).absolute()
        index = self.logger.establecer_tareas(tipo="CARGA", nombre=p.name)
        i = index[0]
        self.logger._print_encabezado()
        self.logger.inicia_consulta(i)
        intento = 0
        self._create_self_folders()
        while intento < self.max_tries:
            intento += 1
            try:
                self.logger.info("Intento {} de {}".format(
                    intento
                    , self.max_tries))
                self._subir_archivo(i, path_local, path_remote, compress=compress)
                intento = self.max_tries
            except Exception as e:
                self.logger.error("Falla el intento {} de {}".format(
                    intento
                    , self.max_tries))
                self.logger.error_query(i, e, False)
                if intento == self.max_tries:
                    self._delete_self_folders()
                    raise
                else:
                    pass
        self._delete_self_folders()
        self.logger.finaliza_consulta(i, mensaje='Finaliza la carga del archivo "{}" a "{}'.format(path_local,
                                                                                                   path_remote))
        self.logger._print_line()

    def bajar_archivo(self, path_remote, path_local):
        """
        Método usado para bajar un archivo del servidor si te encuentras en modo remoto, de lo contrario
        si te encuentras en modo local este método no hace nada

        Parameters
        ----------
        paths_remote : str
            Rutas o nombres de el archivo de origen en el servidor

        paths_local : str
            Rutas o nombres de el archivo que se guardara en local
        """
        p = Path(path_local).absolute()
        index = self.logger.establecer_tareas(tipo="DESCARGA", nombre=p.name)
        i = index[0]
        self.logger._print_encabezado()
        self.logger.inicia_consulta(i)
        intento = 0
        self._create_self_folders()
        while intento < self.max_tries:
            intento += 1
            try:
                self.logger.info("Intento {} de {}".format(
                    intento
                    , self.max_tries))
                self._bajar_archivo(i, path_remote, path_local)
                intento = self.max_tries
            except Exception as e:
                self.logger.error("Falla el intento {} de {}".format(
                    intento
                    , self.max_tries))
                self.logger.error_query(i, e, False)
                if intento == self.max_tries:
                    self._delete_self_folders()
                    raise
                else:
                    pass
        self._delete_self_folders()
        self.logger.finaliza_consulta(i, mensaje='Finaliza la descarga del archivo "{}" a "{}'.format(path_local,
                                                                                                path_remote))
        self.logger._print_line()

    def _submit(self, task_id, archivo, spark_options='', application_arguments='', compress='infer'):
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
        if self.remote:
            path = Path(archivo)  # Ruta local del archivo
            path_remote = '{}/{}'.format(self.folder_server, path.name)  # Ruta en la que quedaria en el servidor

            if "--py-files" in spark_options:
                import shlex
                args = shlex.split(spark_options)
                src_py_files = [Path(py_file) for py_file in args[args.index("--py-files") + 1].split(",")]
                trg_py_files = ['{}/{}'.format(self.folder_server, py_file.name) for py_file in src_py_files]
                args[args.index("--py-files") + 1] = ",".join(trg_py_files)
                spark_options = " ".join(args)
                self._subir_archivos(task_id, src_py_files, trg_py_files, end='', compress=compress)
                self.logger._reportar(task_id, estado="SPARK...")

            command = 'spark-submit {} "{}" {}'.format(spark_options, path_remote, application_arguments)

            self._subir_archivo(task_id, path, path_remote, end='', compress=compress)
            self.logger._reportar(task_id, estado="SPARK...")
            try:
                self.__ejecutar(command, task_id)  # , 'No se pudo realizar la ejecucion correctamente'
            finally:
                self.__ejecutar('rm "{}"'.format(path_remote), task_id, program='Bash',
                                error_pattern='.+')  # , 'No se pudo limpar los archivos del servidor'
                if "--py-files" in spark_options:
                    for py_file in trg_py_files:
                        self.__ejecutar('rm "{}"'.format(py_file), task_id, program='Bash',
                                        error_pattern='.+')  # , 'No se pudo limpar los archivos del servidor'
        else:
            path = Path(archivo)  # Ruta local del archivo
            command = 'spark-submit {} "{}" {}'.format(spark_options, path, application_arguments)

            self.logger._reportar(task_id, estado="SPARK...")
            self.__ejecutar(command, task_id)  # , 'No se pudo realizar la calificacion correctamente'

    def submit(self, archivo, spark_options='', application_arguments='', compress='infer'):
        """
        Metodo usado para ejecutar una aplicacion de Spark ya sea de `Scala` (archivo `.jar`)
        o ya sea de `Python` (archivo `.py`)

        `NOTA`: Si desea que Sparky le notifique algo en particular durante la ejecucion incluya dentro de su código
        algo que imprima el siguiente comando:

        `info_orquestador: (su_notificacion)`

        reemplazando la parte `su_notificacion` por la palabra que Sparky le mostrará.

        Ejm: Si quiere que cuando su script de python vaya a empezar a leer un csv Sparky le informe que esta leyendo, antes de
        iniciar la lectura inserte el comando: `print('info_orquestador: (leyendo)')`

        Parameters
        ----------
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
        p = Path(archivo).absolute()
        index = self.logger.establecer_tareas(tipo="SUBMIT", nombre=p.name)
        i = index[0]
        self.logger._print_encabezado()
        self.logger.inicia_consulta(i)
        intento = 0
        self._create_self_folders()
        while intento < self.max_tries:
            intento += 1
            try:
                self.logger.info("Intento {} de {}".format(
                    intento
                    , self.max_tries))
                self._submit(i, archivo, spark_options, application_arguments, compress=compress)
                intento = self.max_tries
            except Exception as e:
                self.logger.error("Falla el intento {} de {}".format(
                    intento
                    , self.max_tries))
                self.logger.error_query(i, e, False)
                if intento == self.max_tries:
                    self._delete_self_folders()
                    raise
                else:
                    pass
        self._delete_self_folders()
        self.logger.finaliza_consulta(i, mensaje='Finaliza la ejecucion del archivo "{}"'.format(archivo))
        self.logger._print_line()

    def _ejecutar_calificador(self, task_id, modelo, tabla_origen, tabla_destino, zona='proceso', variables=[],
                              spark_options='', modo='error', particion=None):
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
        self.logger._reportar(task_id, estado='cargando')

        # Revisar formato de las variables
        revisar_columnas = [re.match(r'(\w+)(\[(\w+)\])?((\s+as)?\s+(\w+))?$', x, re.IGNORECASE) for x in variables]
        columnas_malas = [x for x, y in zip(variables, revisar_columnas) if not y]
        assert all(revisar_columnas), "Error en el formato de las columnas {}".format(columnas_malas)

        config = {
            'modelo': modelo,
            'tabla_orig': tabla_origen,
            'tabla_dest': tabla_destino,
            'zona': zona,
            'variables': variables,
            'mode': modo,
            'partitionBy': particion
        }
        params = json.dumps(config)
        self._submit(
            task_id,
            '{}/sparky_calificacion.py'.format(self.obtener_ruta()),  # Ruta local del archivo,
            spark_options,
            "'{}'".format(params)
        )
        self.logger._reportar(task_id, estado="invalidate")
        cursor = self.helper.obtener_cursor()
        nombre_tabla = tabla_destino if re.search(r'\.', tabla_destino) else zona + '.' + tabla_destino
        self.helper._ejecutar_consulta(cursor, task_id, 'set SYNC_DDL=1', '')
        self.helper._ejecutar_consulta(cursor, task_id, 'invalidate metadata ' + nombre_tabla, '')
        self.helper._ejecutar_consulta(cursor, task_id, 'refresh ' + nombre_tabla, '')
        self.helper._ejecutar_consulta(cursor, task_id, 'set SYNC_DDL=0', '')

    def ejecutar_calificador(self, modelo, tabla_origen, tabla_destino, zona='proceso', variables=[], spark_options='',
                             modo='error', particion=None):
        """
        Método util para poder ejecutar un modelo de `Spark` que la salida sea una tabla que se pueda
        subir a la LZ

        Parameters
        ----------
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
        p = Path(modelo).absolute()
        index = self.logger.establecer_tareas(tipo="CALIF.", nombre=p.name)
        i = index[0]
        self.logger._print_encabezado()
        self.logger.inicia_consulta(i)
        intento = 0
        self._create_self_folders()
        while intento < self.max_tries:
            intento += 1
            try:
                self.logger.info("Intento {} de {}".format(
                    intento
                    , self.max_tries))
                self._ejecutar_calificador(i, modelo, tabla_origen, tabla_destino, zona, variables, spark_options, modo,
                                           particion)
                intento = self.max_tries
            except Exception as e:
                self.logger.error("Falla el intento {} de {}".format(
                    intento
                    , self.max_tries))
                self.logger.error_query(i, e, False)
                if intento == self.max_tries:
                    self._delete_self_folders()
                    raise
                else:
                    pass
        self._delete_self_folders()
        self.logger.finaliza_consulta(i,
                                      mensaje='Finaliza la calificacion con el modelo "{}", resultados disponibles en: "{}'.format(
                                          modelo, tabla_destino))
        self.logger._print_line()

    def _subir_csv(self, task_id, path, nombre_tabla, zona='proceso', end='\n', spark_options='', compress='infer',
                   modo='error', particion=None, **kwargs):
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

        # Modificar parametro de Spark por defecto
        kwargs['header'] = kwargs.get('header', True)
        if 'schema' not in kwargs:
            kwargs['inferSchema'] = kwargs.get('inferSchema', True)

        # Configuracion para spark
        path = Path(path)  # Se hace para luego convertirla a ruta absoluta si es necesario
        hdfs_path = '{}/{}'.format(self.folder_hdfs, path.name)  # Ruta del CSV en el HDFS

        config = {
            'ruta_origen': hdfs_path,
            'nombre_tabla': nombre_tabla,
            'zona': zona,
            'tipo': 'csv',
            'mode': modo,
            'partitionBy': particion,
            'kwargs': kwargs
        }
        params = json.dumps(config)

        try:
            # Determinar las rutas a los diferentes archivos
            if self.remote:
                file_path = '{}/{}'.format(self.folder_server, path.name)  # Ruta del CSV en el servidor

                if path.is_dir():
                    self.__ejecutar('mkdir -p "{}"'.format(file_path), program='Bash', error_pattern='.+')
                    # Se suben todos los archivos de la carpeta
                    paths_local = list(path.iterdir())
                    paths_remote = ["{}/{}".format(file_path, x.name) for x in paths_local]
                    self._subir_archivos(task_id, paths_local, paths_remote, end='', compress=compress)
                else:
                    # Se sube el archivo CSV
                    paths_local = [path.absolute()]
                    paths_remote = [file_path]
                    self._subir_archivos(task_id, paths_local, paths_remote, end='', compress=compress)
            else:
                file_path = str(path.absolute())  # Ruta del CSV en el servidor
            # Ejecutar los archivos para subir la tabla a impala y dejarla disponible
            self.logger._reportar(task_id, estado="subir HDFS")
            new_file_path = file_path.replace(' ', '%20')  # Es necesario reemplazar los espacios para subir al HDFS
            self.__ejecutar(
                'hdfs dfs -put -f "{}" "{}"'.format(new_file_path, hdfs_path),
                task_id,
                program='HDFS',
                error_pattern='.+'
            )
            self.logger._reportar(task_id, estado="SPARK...")
            self._submit(
                task_id,
                '{}/sparky_subir_tabla.py'.format(self.obtener_ruta()),  # Ruta local del archivo,
                spark_options,
                "'{}'".format(params)
            )

            self.logger._reportar(task_id, estado="invalidate")
            cursor = self.helper.obtener_cursor()
            tabla_destino = nombre_tabla if re.search(r'\.', nombre_tabla) else zona + '.' + nombre_tabla
            self.helper._ejecutar_consulta(cursor, task_id, 'set SYNC_DDL=1', '')
            self.helper._ejecutar_consulta(cursor, task_id, 'invalidate metadata ' + tabla_destino, '')
            self.helper._ejecutar_consulta(cursor, task_id, 'refresh ' + tabla_destino, '')
            self.helper._ejecutar_consulta(cursor, task_id, 'set SYNC_DDL=0', '')

        finally:
            self.logger._reportar(task_id, estado='limpiando')
            if self.remote:
                try:
                    # Eliminar los archivos subidos (no dejar basura)
                    if path.is_dir():
                        for archivo in paths_remote:
                            self.__ejecutar('rm "{}"'.format(archivo), program='Bash', error_pattern='.+')
                        self.__ejecutar('rm -d "{}"'.format(file_path), program='Bash', error_pattern='.+')
                    else:
                        self.__ejecutar('rm "{}"'.format(file_path), task_id, program='Bash', error_pattern='.+')
                except:
                    self.logger.print()
                    self.logger.warning("No se lograron limpiar los archivos del servidor")

            try:
                if path.is_dir():
                    self.__ejecutar('hdfs dfs -rm -r "{}"'.format(hdfs_path), task_id, program='HDFS',
                                    error_pattern='.+')
                else:
                    self.__ejecutar('hdfs dfs -rm "{}"'.format(hdfs_path), task_id, program='HDFS', error_pattern='.+')
            except:
                self.logger.print()
                self.logger.warning("No se lograron limpiar los archivos del HDFS")

    def subir_csv(self, path, nombre_tabla, zona='proceso', spark_options='', compress='infer', modo='error',
                  particion=None, **kwargs):
        """
        Método util para montar en la LZ una tabla que se encuentre en un archivo `CSV`, Se le pueden pasar los parametros
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
        index = self.logger.establecer_tareas(tipo="CSV A LZ", nombre=nombre_tabla)
        i = index[0]
        self.logger._print_encabezado()
        self.logger.inicia_consulta(i)
        intento = 0
        self._create_self_folders()
        while intento < self.max_tries:
            intento += 1
            try:
                self.logger.info("Intento {} de {}".format(
                    intento
                    , self.max_tries))
                self._subir_csv(
                    i,
                    path,
                    nombre_tabla,
                    zona,
                    spark_options=spark_options,
                    compress=compress,
                    modo=modo,
                    particion=particion,
                    **kwargs
                )
                intento = self.max_tries
            except Exception as e:
                self.logger.error("Falla el intento {} de {}".format(
                    intento
                    , self.max_tries))
                self.logger.error_query(i, e, False)
                if intento == self.max_tries:
                    self._delete_self_folders()
                    raise
                else:
                    pass

        self._delete_self_folders()
        self.logger.finaliza_consulta(i, mensaje="Finaliza la creacion de tabla en la LZ")
        self.logger._print_line()

    def _subir_excel(self, task_id, path, nombre_tabla, zona='proceso', end='\n', spark_options='', compress='infer',
                     modo='error', particion=None, **kwargs):
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

        # Configuracion para spark
        path = Path(path)  # Se hace para luego convertirla a ruta absoluta si es necesario
        config = {
            'nombre_tabla': nombre_tabla,
            'zona': zona,
            'tipo': 'excel',
            'mode': modo,
            'partitionBy': particion,
            'kwargs': kwargs
        }

        try:
            # Determinar las rutas al archivo
            if self.remote:
                file_path = '{}/{}'.format(self.folder_server, path.name)  # Ruta del CSV en el servidor
                self._subir_archivo(task_id, path.absolute(), file_path, end='', compress=compress)  # Se sube CSV
            else:
                file_path = str(path.absolute())  # Ruta del CSV en el servidor

            config['ruta_origen'] = file_path
            params = json.dumps(config)
            # Ejecutar los archivos para subir la tabla a impala y dejarla disponible
            self.logger._reportar(task_id, estado="SPARK...")
            self._submit(
                task_id,
                '{}/sparky_subir_tabla.py'.format(self.obtener_ruta()),  # Ruta local del archivo,
                spark_options,
                "'{}'".format(params)
            )

            self.logger._reportar(task_id, estado="invalidate")
            cursor = self.helper.obtener_cursor()
            tabla_destino = nombre_tabla if re.search(r'\.', nombre_tabla) else zona + '.' + nombre_tabla
            self.helper._ejecutar_consulta(cursor, task_id, 'set SYNC_DDL=1', '')
            self.helper._ejecutar_consulta(cursor, task_id, 'invalidate metadata ' + tabla_destino, '')
            self.helper._ejecutar_consulta(cursor, task_id, 'refresh ' + tabla_destino, '')
            self.helper._ejecutar_consulta(cursor, task_id, 'set SYNC_DDL=0', '')
        finally:
            self.logger._reportar(task_id, estado='limpiando')
            if self.remote:
                try:
                    # Eliminar los archivos subidos (no dejar basura)
                    self.__ejecutar('rm "{}"'.format(file_path), task_id, program='Bash', error_pattern='.+')
                except:
                    self.logger.warning("No se lograron limpiar los archivos del servidor")

    def subir_excel(self, path, nombre_tabla, zona='proceso', spark_options='', compress='infer', modo='error',
                    particion=None, **kwargs):
        """
        Método util para montar en la LZ una tabla que se encuentre en un archivo `CSV`, Se le pueden pasar los parametros
        usados por Spark para configurar la subida, disponibles en:
        https://pandas.pydata.org/pandas-docs/version/0.20.3/generated/pandas.read_excel.html

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
        index = self.logger.establecer_tareas(tipo="XLS A LZ", nombre=nombre_tabla)
        i = index[0]
        self.logger._print_encabezado()
        self.logger.inicia_consulta(i)
        intento = 0
        self._create_self_folders()
        while intento < self.max_tries:
            intento += 1
            try:
                self.logger.info("Intento {} de {}".format(
                    intento
                    , self.max_tries))
                self._subir_excel(
                    i,
                    path,
                    nombre_tabla,
                    zona,
                    spark_options=spark_options,
                    compress=compress,
                    modo=modo,
                    particion=particion,
                    **kwargs
                )
                intento = self.max_tries
            except Exception as e:
                self.logger.error("Falla el intento {} de {}".format(
                    intento
                    , self.max_tries))
                self.logger.error_query(i, e, False)
                if intento == self.max_tries:
                    self._delete_self_folders()
                    raise
                else:
                    pass
        self._delete_self_folders()
        self.logger.finaliza_consulta(i, mensaje="Finaliza la creacion de tabla en la LZ")
        self.logger._print_line()

    def _subir_parquet(self, task_id, path, nombre_tabla, zona='proceso', end='\n', spark_options='', compress='infer',
                     modo='error', particion=None, **kwargs):
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
        path = Path(path)  # Se hace para luego convertirla a ruta absoluta si es necesario
        hdfs_path = '{}/{}'.format(self.folder_hdfs, path.name)
        config = {
            'ruta_origen': hdfs_path,
            'nombre_tabla': nombre_tabla,
            'zona': zona,
            'tipo': 'parquet',
            'mode': modo,
            'partitionBy': particion,
            'kwargs': kwargs
        }
        params = json.dumps(config)
        try:
            # Determinar las rutas a los diferentes archivos
            if self.remote:
                file_path = '{}/{}'.format(self.folder_server, path.name)  # Ruta del parquet en el servidor

                if path.is_dir():
                    self.__ejecutar('mkdir -p "{}"'.format(file_path), program='Bash', error_pattern='.+')
                    # Se suben todos los archivos de la carpeta
                    paths_local = list(path.iterdir())
                    paths_remote = ["{}/{}".format(file_path, x.name) for x in paths_local]
                    self._subir_archivos(task_id, paths_local, paths_remote, end='', compress=compress)
                else:
                    # Se sube el archivo Parquet
                    paths_local = [path.absolute()]
                    paths_remote = [file_path]
                    self._subir_archivos(task_id, paths_local, paths_remote, end='', compress=compress)
            else:
                file_path = str(path.absolute())  # Ruta del Parquet en el servidor
            # Ejecutar los archivos para subir la tabla a impala y dejarla disponible
            self.logger._reportar(task_id, estado="subir HDFS")
            new_file_path = file_path.replace(' ', '%20')  # Es necesario reemplazar los espacios para subir al HDFS
            self.__ejecutar(
                'hdfs dfs -put -f "{}" "{}"'.format(new_file_path, hdfs_path),
                task_id,
                program='HDFS',
                error_pattern='.+'
            )
            self.logger._reportar(task_id, estado="SPARK...")

            self._submit(
                task_id,
                '{}/sparky_subir_tabla.py'.format(self.obtener_ruta()),  # Ruta local del archivo,
                spark_options,
                "'{}'".format(params)
            )

            self.logger._reportar(task_id, estado="invalidate")
            cursor = self.helper.obtener_cursor()
            tabla_destino = nombre_tabla if re.search(r'\.', nombre_tabla) else zona + '.' + nombre_tabla
            self.helper._ejecutar_consulta(cursor, task_id, 'set SYNC_DDL=1', '')
            self.helper._ejecutar_consulta(cursor, task_id, 'invalidate metadata ' + tabla_destino, '')
            self.helper._ejecutar_consulta(cursor, task_id, 'refresh ' + tabla_destino, '')
            self.helper._ejecutar_consulta(cursor, task_id, 'set SYNC_DDL=0', '')
        finally:
            self.logger._reportar(task_id, estado='limpiando')
            if self.remote:
                try:
                    # Eliminar los archivos subidos (no dejar basura)
                    self.__ejecutar('rm "{}"'.format(file_path), task_id, program='Bash', error_pattern='.+')
                except:
                    self.logger.warning("No se lograron limpiar los archivos del servidor")

    def _subir_df(self, task_id, df, nombre_tabla, zona='proceso', end='\n', spark_options='', compress='infer',
                  modo='error', particion=None, parquet=False, **kwargs):
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
        if parquet == True:
            with tempfile.TemporaryDirectory() as tempfolder:
                self.logger._reportar(task_id, estado='convirtiendo')
                file = '{}/tabla_{}.csv'.format(tempfolder, self.nombre)
                df[df.select_dtypes(include=['object']).columns] = df.select_dtypes(include=['object']).astype(str) #conversión de objet a string
                df=df.replace("nan", None) # remplazar nan a None parque en la LZ quede como Null
                df.to_parquet(file)
                self._subir_parquet(task_id, file, nombre_tabla, zona, end, spark_options, compress, modo, particion, **kwargs)
        elif parquet == False:
            with tempfile.TemporaryDirectory() as tempfolder:
                self.logger._reportar(task_id, estado='convirtiendo')
                file = '{}/tabla_{}.csv'.format(tempfolder, self.nombre)
                df.to_csv(file, index=False, encoding='utf8')
                kwargs['encoding'] = 'utf8'
                self._subir_csv(task_id, file, nombre_tabla, zona, end, spark_options, compress, modo, particion, **kwargs)

    def subir_df(self, df, nombre_tabla, zona='proceso', spark_options='', compress='infer', modo='error',
                 particion=None, parquet=False, **kwargs):
        """
        Método util para montar en la LZ una tabla que se encuentre en un DataFrame de pandas, Se le pueden pasar los parametros
        usados por Spark para configurar la subida, disponibles en:
        https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=read%20csv#pyspark.sql.DataFrameReader.csv

        Parameters
        ----------
        df : str
            Nombre o ruta del archivo `CSV` a subir

        nombre_tabla : str
            Nombre de la tabla de como quedará en la LZ. Se puede especificar la zona en la que será guardado, si no se especifica
            será guardada en la zona del parámetro `zona`. `NOTA`: Si la tabla existe será sobrescrita.
            usela con cuidado.

        zona : str, Opcional, Default: `proceso`
            Zona en la que será guardada la base si no se especifica dentro del parametro `nombre_tabla`

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
        index = self.logger.establecer_tareas(tipo="DF A LZ", nombre=nombre_tabla)
        i = index[0]
        self.logger._print_encabezado()
        self.logger.inicia_consulta(i)
        intento = 0
        self._create_self_folders()
        while intento < self.max_tries:
            intento += 1
            try:
                self.logger.info("Intento {} de {}".format(
                    intento
                    , self.max_tries))
                self._subir_df(
                    i,
                    df,
                    nombre_tabla,
                    zona,
                    spark_options=spark_options,
                    compress=compress,
                    modo=modo,
                    particion=particion,
                    parquet=parquet,
                    **kwargs
                )
                intento = self.max_tries
            except Exception as e:
                self.logger.error("Falla el intento {} de {}".format(
                    intento
                    , self.max_tries))
                self.logger.error_query(i, e, False)
                if intento == self.max_tries:
                    self._delete_self_folders()
                    raise
                else:
                    pass
        self._delete_self_folders()
        self.logger.finaliza_consulta(i, mensaje="Finaliza la creacion de tabla en la LZ")
        self.logger._print_line()

    def _ejecucion_remota(self, command, loggeador):
        """
        Método interno para ejecucion de comandos Linux en los servidores del banco

        Parameters
        ----------
        command : str
            Comando Linux a ejecutar en el servidor

        loggeador : Loggeador
            Instancia dedicada a verificar y reportar en el archivo de log la salida de
            Spark en el mismo nivel que es reportado por este

        Raise
        -----
            `Exception` si el comando no finalizo correctamente
        """
        with self.__remote.connect(hostname=self.hostname, port=self.port) as client:
            stdin, stdout, stderr = client.exec_command(command, get_pty=True)

            for line in stdout:
                line = line.rstrip()
                loggeador.loggear(line, self.show_outp)

            response = stdout.channel.recv_exit_status()
        del stdin, stdout, stderr, client
        if not response == 0:
            raise loggeador.exception()

    def _ejecucion_local(self, command, loggeador):
        """
        Método interno para ejecucion de comandos Linux en un subproceso. Pensado para ejecutar comandos
        cuando se esten ejecutando dentro de los mismos servidores del banco y no de manera remota.

        Parameters
        ----------
        task_id : int
            id de la tarea a la cual pertenece el comando

        command : str
            Comando Linux a ejecutar en Bash

        loggeador : Loggeador
            Instancia dedicada a verificar y reportar en el archivo de log la salida de
            Spark en el mismo nivel que es reportado por este

        Raise
        -----
            `Exception` si el comando no finalizo correctamente
        """
        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
        for line in p.stdout:
            line = line.decode(errors='replace').rstrip()
            loggeador.loggear(line, self.show_outp)
        if not p.wait() == 0:
            raise loggeador.exception()

    def __ejecutar(self, command, task_id=None, program='Spark',
                   error_pattern=r'\d\d/\d\d/\d\d \d\d:\d\d:\d\d (ERROR)|Error'):
        """
        Método privado usado para ejecutar un comando Linux indistintamente si se encuentre en modo remoto
        o local

        Parameters
        ----------
        command : str
            Comando Linux a ejecutar

        task_id : int, Opcional, Default: None
            id de la tarea a la cual pertenece el comando

        Raise
        -----
            `Exception` si el comando no finalizo correctamente
        """

        class Loggeador:
            def __init__(self, logger, task_id, program, error_pattern):
                self.logger = logger
                self.task_id = task_id
                self.level_actual = 'INFO'
                self.program = program
                self.error_pattern = error_pattern
                self.msj_error = ["{} ERROR:".format(self.program)]
                self.reportar = {
                    'DEBUG': self.logger.log.debug,
                    'INFO': self.logger.log.info,
                    'WARN': self.logger.log.warning,
                    'ERROR': self.logger.log.error,
                    'FATAL': self.logger.log.critical,
                }

            def loggear(self, line, show_output):
                # Para loggear en el archivo
                levels = '|'.join(self.reportar)
                new_level = re.match(r'\d\d/\d\d/\d\d \d\d:\d\d:\d\d ({})'.format(levels), line, re.IGNORECASE)
                errors = re.match(self.error_pattern, line, re.IGNORECASE)
                ignore = re.match('Password for', line, re.IGNORECASE)
                if ignore:
                    return

                if new_level:
                    if self.level_actual != new_level.group(1):  # Si cambia de nivel
                        self.level_actual = new_level.group(1)
                        self.msj_error = ["{} ERROR:".format(self.program)]
                elif re.match(r'Traceback \(most recent call last\)', line, re.IGNORECASE):
                    if self.level_actual != 'ERROR':
                        self.level_actual = 'ERROR'
                        self.msj_error = ["{} ERROR:".format(self.program)]
                if errors and self.level_actual != 'ERROR':
                    self.level_actual = 'ERROR'
                    self.msj_error = ["{} ERROR:".format(self.program)]

                self.reportar[self.level_actual](line)
                if self.level_actual in {'ERROR', 'FATAL'}:
                    self.msj_error.append(line)

                # Para mostrar en consola
                if (not show_output) and (self.task_id is not None):
                    estado = re.match(r'info_orquestador: \((.*)\)', line, re.IGNORECASE)
                    if estado:
                        self.logger._reportar(task_id, estado=estado.group(1))
                elif show_output:
                    print(line)

            def exception(self):
                return Exception('\n    '.join(self.msj_error))

        r = Loggeador(self.logger, task_id, program, error_pattern)
        if self.remote:
            self._ejecucion_remota(command, r)
        else:
            self._ejecucion_local(command, r)

    def obtener_ruta(self):
        return pkg_resources.resource_filename(__name__, 'static')

    def ejecutar_archivo_sql(self, path, params=None, max_tries=2, spark_options=''):
        """
        Método público para ejecutar consultas SQL HIVE que se encuentran dentro de un archivo .sql mediante spark.sql

        Parameters
        ----------

        path : str
            Ruta del archivo que contiene las consultas SQL HIVE

        params : dict, default=None
            Diccionario de parámetros para ser reemplazados en las consultas


        Raise
        -----
            `Exception` si el comando no finalizo correctamente

        """
        _, queries = self.helper._obtener_consultas(path, params)
        self.logger.df = self.logger.df[self.logger.df['documento'] != Path(path).absolute().name]

        index = self.logger.establecer_tareas(tipo="SQL", nombre=Path(path).absolute().name)
        i = index[0]
        self.logger._print_encabezado()
        self.logger.inicia_consulta(i)
        self._create_self_folders()
        try:
            self._ejecutar_consultas_sql(i, queries, max_tries, spark_options=spark_options)
        except Exception as e:
            self._delete_self_folders()
            self.logger.error_query(i, e, False)
            raise
        self._delete_self_folders()
        self.logger.finaliza_consulta(i, mensaje="Finaliza la ejecución de consultas SQL")
        self.logger._print_line()

    def _ejecutar_consultas_sql(self, task_id, queries, max_tries=1, spark_options=''):
        """
        Método semi-privado para ejecutar una lista de consultas SQL HIVE
        """
        conf = {
            'queries': queries,
            'max_tries': max_tries
        }
        config = json.dumps(conf)

        self._submit(
            task_id,
            '{}/sparky_ejecutar_sql.py'.format(self.obtener_ruta()),  # Ruta local del archivo,
            spark_options,
            "'{}'".format(config)
        )
