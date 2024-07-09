# -*- coding: utf-8 -*-
"""
Created on Wed Aug 18 11:47:43 2021

@author: RLARIOS
"""

from orquestador2.logger import Logger
from orquestador2.emailer import Emailer
from orquestador2._version import get_versions

from getpass import getuser
import sys
import os
import platform
import traceback

import time
from datetime import datetime
from pytz import timezone
import pandas as pd

from helper.helper import Helper

# Nueva condición para importar una u otra libreria dependiendo de la maquina
from sparky_bc import Sparky

if os.environ.get('MAQUINA','') == 'CDP-PC':
    from sparky_bc.sparky_cdp import SparkyCDP

    Sparky = SparkyCDP


class Orchestrator:
    """ Clase que permite la ejecución ordenada y consecutiva de una serie de pasos que se encargan
    de ejecutar acciones sobre la plataforma analítica.  Entrega funciones de ayuda que permiten configurar
    el comportamiento de la ejecución y entrega a los pasos los logs y helpers
    necesarios para la ejecución de actividades en la LZ.
    """

    def __init__(self, name, steps, **kwargs):
        """Constructor de la clase Orchestrator, se encarga de recibir la lista de pasos.

        Recibe como parámetros obligatorios:
            name : (str) el nombre del proceso a orquestar
            steps : (list) lista de elementos (steps) a ejecutar consecutivamente, Deben ser subclases del tipo Step.

        """

        self.log = None
        self.helper = None
        self.sparky = None
        self.initialized = False
        self.globalConfig = None
        self.packageName = None
        self.maxTries = 1
        self.waitPeriod = 600
        self.emailer = None
        self.keyList = ["dsn"]

        # Variable utilizada para repisar información de la configuración estática
        self.kw = kwargs

        self.tz = timezone(self.kw.get("timeZone", "America/Bogota"))

        if name is None:
            raise ValueError("Debe enviar nombre para este orquestador")
        if type(name) != str:
            raise ValueError("El valor del nombre debe ser de tipo String")

        if steps is None:
            raise ValueError("Debe enviar una lista de pasos para este orquestador")

        self.log_type = self.kw.get('log_type', '').lower().strip()
        if self.log_type not in ['', 'est', 'cmp']:
            raise ValueError("El tipo de log permitido es : '', 'est', 'cmp'")

        self.log_path = self.kw.get('log_path', './logs/')

        self.name = name

        self.stats = []
        self.columns = ["paso", "inicio", "fin", "duracion", "estado", "info"]

        self.statsDF = None
        self.format = self.kw.get("dateFormat", "%Y-%m-%d %H:%M:%S")
        self.formatDate = self.kw.get("formatDate", "%Y%m%d")
        self.formatTime = self.kw.get("formatTime", "%H:%M:%S")

        self.validateSteps(steps)
        self.steps = steps
        self.initializeSteps()
        self.executionRecord = {}
        self.query_record = "insert into resultados.ejecuciones_orquestador2 values ( {fecha_inicio} , '{hora_inicio}' , '{nomb_paquete}' ,  '{nomb_proceso}' , {fecha_fin},  '{hora_fin}' , {duracion},  '{estado_fin}' , \"{texto_fin}\" ) "
        self.printVersion()

    def printVersion(self):
        verDic = {}
        try:
            verDic = get_versions()
        except:
            verDic = {"version": "Not Found"}

        version = verDic.get("version", "Not Found")
        self.log.info("Versión de Orquestador: {0}".format(version))

    def setSteps(self, steps):
        """Define los pasos a ejecutar por el orquestador , valida si el tipo entregado es válido y los asigna internamente

        Parameters
        ----------
        steps : List
            lista de elementos (Clase Step) a ejecutar consecutivamente, Deben ser subclases del tipo Step.

        Raises
        ------
        ValueError
            Cuando la lista es None o cuando hay error en la validación de la lista de pasos (lista vacia o elementos distintos a Step.

        Returns
        -------
        None.

        """
        if steps == None:
            raise ValueError("Debe enviar una lista de pasos no vacía")

        self.validateSteps(steps)
        self.steps = steps
        self.initializeSteps()

    def validateSteps(self, steps):
        """Valida que los pasos sean del tipo correcto

        Parameters
        ----------
        steps : List
            lista de elementos (clase Step) a ejecutar consecutivamente, Deben ser subclases del tipo Step..

        Raises
        ------
        TypeError
            Cuando el elemento enviado no es una lista.
        ValueError
            Cuando la lista está vacía o alguno de los elementos no es de clase Step.

        Returns
        -------
        None.

        """
        if steps is not None and type(steps) != list:
            raise TypeError("El tipo de los pasos debe ser una lista")

        if steps is not None and type(steps) == list:

            if len(steps) == 0:
                raise ValueError("La lista de pasos debe ser no vacía")

            for i, step in enumerate(steps):
                try:
                    step.verifyIamAStep()
                except:
                    raise ValueError("El paso {0} de lista no es de tipo Step, tipo enviado: {1}".format(i, type(step)))

    def checkObligatoryKeys(self):
        """Chequea que la configuración global contenga elementos obligatorios necesarios para la instanciación

        Raises
        ------
        ValueError
            Cuando uno de los elementos de la lista de llaves no se encuentra en la configuración global enviada.

        Returns
        -------
        None.

        """
        for k in self.keyList:
            if k not in self.globalConfig:
                raise ValueError(
                    "la configuración '{0}' debe estar en la configuración global.  Las llaves necesarias son: {1}".format(
                        k, self.keyList))

    def initializeHelpers(self):
        """Se encarga de inicializar los Helpers que podrán utilizar los distintos pasos.  Estos solo se inicializan una vez y les sirven a todos
        los componentes de los pasos.

        Se utiliza la Configuración global para ello

        Returns
        -------
        None.

        """

        self.log = Logger(self.name, path=self.log_path, log_type=self.log_type)
        
        self.emailer = Emailer( self.log , **self.globalConfig )

        # Se define una configuración especifica, el resto de valores se envían como están en la configuración
        # Sparky no recibe Kwargs por lo que es necesario definir las variables "a mano"

        if self.globalConfig.get("useSparky", True):
            # Por defecto usa Sparky con Impala Helper

            spkConfig = {}

            spkConfig["logger"] = self.log
            spkConfig["dsn"] = self.globalConfig["dsn"]
            if os.environ.get('MAQUINA','') == 'CDP-PC':
                spkConfig["cdp_endpoint"] = self.globalConfig["cdp_endpoint"]

            spkConfig["username"] = self.validInput("username", getuser(), str)
            if spkConfig["username"] == "srv_svccdp03":
                spkConfig["password"] = os.environ["DECRYPT_USER"]
            else:
                spkConfig["password"] = self.globalConfig.get("password", None)
            spkConfig["remote"] = self.globalConfig.get("remote", "infer")
            spkConfig["hostname"] = self.validInput("hostname", "sbmdeblze004.bancolombia.corp", str)
            spkConfig["port"] = self.validInput("port", 22, int)
            spkConfig["show_outp"] = self.validInput("show_outp", False, bool)
            spkConfig["max_tries"] = self.validInput("max_tries", 3, int)

            self.sparky = Sparky(**spkConfig)
            self.helper = self.sparky.helper

            self.helper.actualizar_configuracion(self.globalConfig)

        else:
            # En este caso el coordinador solo se conecta a Impala
            hlpConfig = {}
            hlpConfig.update(self.globalConfig)

            hlpConfig["logger"] = self.log
            self.helper = Helper(**hlpConfig)

    def validInput(self, param, default, theType):
        """Funcion que permite validar si un elemento eniado en la configuración es del tipo válido

        Parameters
        ----------
        param : Str
            Nombre del parámetro a evaluar.
        default : (Any)
            Valor por defecto que se debería devolver.
        theType : TYPE
            Tipo del elemento a validar.

        Raises
        ------
        Exception
            Cuando el valor enviado en el diccionario no corresponde al tipo esperado.

        Returns
        -------
        value : ANY
            valor entregado por el diccionario, ya se que lo encuentre o el valor por defecto.

        """
        value = self.globalConfig.get(param, default)
        if type(value) != theType:
            msg = "Error de instanciación de Orquestador para el parámetro '{0}': Tipo Enviado: {1}, tipo Esperado: {2}".format(
                param, type(value), theType)
            raise Exception(msg)

        return value

    def resetParameters(self):
        """Función que resetea los valorees por defecto para la ejecución del orquestador
        en caso que sean enviados distintos en la configuración global

        Returns
        -------
        None.

        """
        self.maxTries = self.validInput("maxTries", self.maxTries, int)
        self.waitPeriod = self.validInput("waitPeriod", self.waitPeriod, int)

    def initializeSteps(self):
        """ Se encarga de inicialiar cada paso de la lista añadiendo los helper a cada paso

        Raises
        ------
        ValueError
            En el caso de que no se entregue un diccionario de configuración global.

        Returns
        -------
        None.

        """
        for sx in self.steps:

            if self.globalConfig == None:
                cfg = sx.initGlobalConfiguration()
                if cfg is not None and type(cfg) == dict:
                    self.globalConfig = cfg

                    for k, v in self.kw.items():
                        # repisa los valores staticos por los enviados si estos son enviados
                        self.globalConfig[k] = v

                    self.checkObligatoryKeys()
                    self.resetParameters()
                    self.initializeHelpers()

                else:
                    raise ValueError(
                        "El orquestador debe tener un diccionario de configuración global en la carpeta static (config.json)")

            sx.setModules(self.helper, self.sparky, self.log , self.emailer)
            sx.setGlobalConfig(self.globalConfig)

        self.initialized = True

    def reStarter(self, step, tries=1, maxTries=None):
        """ Funcion que permite reiniciar un paso en el caso que haya un error

        Parameters
        ----------
        step : Step
            Paso a ejecutar.
        tries : int, optional
            EL numero de veces que está ejecutando actualmente. The default is 1.
        maxTries : int, optional
            El número máximo de veces que se debe ejecutar este paso. The default is 1.

        Raises
        ------
        ex
            En el caso que haya un error con el paso y se haya alcanzado el numero máximo de intentos.

        Returns
        -------
        None

        """

        if maxTries == None:
            maxTries = self.maxTries

        try:
            step.ejecutar()

        except Exception as ex:
            if tries >= maxTries:
                self.log.error("Error: Falló el paso {1}, Error Encontrado: {0}".format(ex, type(step).__name__))
                raise ex

            tri = tries + 1
            self.log.info(
                "Error: Falló el paso {2}, Intentando por vez No. {0}, Error Encontrado: {1} ".format(tries, ex, type(
                    step).__name__))
            self.log.info("Durmiendo por {0} segundos".format(self.waitPeriod))
            time.sleep(self.waitPeriod)
            self.log.info("Reintentando paso {0} desde el principio".format(type(step).__name__))

            return self.reStarter(step, tries=tri, maxTries=maxTries)

    def log_exception(self, ex):
        """Función Helper que permite hacer un stack de mensajes especificando el lugar de error en el código
        La información obtenida se escribe en el log como mensajes de error

        Parameters
        ----------
        ex : Exception
            Excepción Capturada.

        Returns
        -------
        None.

        """
        ex_type, ex_value, ex_traceback = sys.exc_info()
        trace_back = traceback.extract_tb(ex_traceback)

        # Format stacktrace
        stack_trace = []

        for trace in trace_back:
            stack_trace.append(
                "File : %s , Line : %d, Func.Name : %s, Message : %s" % (trace[0], trace[1], trace[2], trace[3]))

        msg = 'Error en {0}'.format(self.name)
        self.log.error(msg)
        msg = "Tipo de Excepcion : {0} ".format(ex_type.__name__)
        self.log.error(msg)
        msg = "Mensaje : {0}".format(ex_value)
        self.log.error(msg)
        self.log.error("Stack Trace:")
        for stack in stack_trace:
            msg = "----> {0}".format(stack)
            self.log.error(msg)

    def __genStats(self, paso, inicio, fin, duracion, estado, info):
        """Genera estadisticas de ejecución del paso y actualiza el Dataframe que las ejecuta.

        Parameters
        ----------
        paso : String
            Nombre del paso ejecutado.
        inicio : String
            Fecha en formato %Y-%m-%d %H:%M:%S.
        fin : Str
            Fecha en formato %Y-%m-%d %H:%M:%S.
        duracion : Float
            Duración en segundos del paso.
        estado : String
            Estado del paso, puede ser "OK" o "ERROR"
        info : String
            Cadena vacía si el anterior es OK o El log del error en el caso que haya fallo.

        Returns
        -------
        None.

        """
        self.stats.append([paso, inicio, fin, duracion, estado, info])
        self.statsDF = pd.DataFrame.from_records(self.stats, columns=self.columns)

    def __currentTime(self):
        """
        Genera la fecha en formato definido y en el timezone especificado

        Returns
        -------
        STR
            Fecha en formato "%Y-%m-%d %H:%M:%S"

        """
        return datetime.now(self.tz).strftime(self.format)

    def __currentDateTime(self):
        """
        Genera la fecha en formato definido y en el timezone especificado

        Returns
        -------
        Tuple(STR,STR)
            Fecha en formato "%Y%m%d"
            Hora en formato "%H:%M:%S"

        """
        now = datetime.now(self.tz)
        fecha = now.strftime(self.formatDate)
        hora = now.strftime(self.formatTime)

        return fecha, hora

    def __getPackageName(self, path):
        """Obtiene el nombre del paquete ejecutado teniendo en cuenta el path del archivo que llama al orquestador

        Parameters
        ----------
        path : String
            Ruta del archivo que ejecuta el orquestador

        Returns
        -------
        None.

        """
        modulo = "No_Encontrado"
        try:
            modulo = path.split(os.sep)[-2]
        except:
            self.log.info("No se pudo obtener el nombre del paquete, path enviado: {0}".format(path))

        self.packageName = modulo

    def __startExecRecord(self):
        """Se encarga de guardar los datos del inicio de la ejecución para el registro de la ejecución

        Returns
        -------
        None.

        """
        fecha, hora = self.__currentDateTime()
        self.executionRecord["nomb_paquete"] = self.packageName
        self.executionRecord["nomb_proceso"] = self.name.replace('\'', '\\\'')
        self.executionRecord["fecha_inicio"] = int(fecha)
        self.executionRecord["hora_inicio"] = hora
        self.executionRecord["t1"] = time.time()

    def __saveRecord(self):
        """Imprime el registro de la ejecución en el log.  En el caso que se ejecute en linux (producción)
        insertará el registro en la tabla resultados.ejecuciones_orquestador2 , en cualquier otro SO no lo hará

        Returns
        -------
        None.

        """
        opSys = platform.system()
        self.log.info("Registro de la Ejecución: {0}".format(self.executionRecord))

        try:
            # En el caso de que sea Linux, Intentará enviar el registro a resultados
            if opSys.lower().strip() == "linux":
                self.helper.ejecutar_consulta(self.query_record, params=self.executionRecord)

        except Exception as e:
            self.log.info("WARNING:  No se pudo almacenar el registro de la ejecución del orquestador")
            self.log.info("WARNING:  Error encontrado: {0}".format(e))
            self.log.info(
                "WARNING:  Esto no es un error de la rutina, sino del almacenamiento del registro de ejecución")

    def __endExecRecord(self, exception=None):
        """Se encarga de guardar los datos del finalización de la ejecución
        ya sea que haya finalizado correctamente o con error (Con error es si se envía una excepción).
        Al final envía a guardar el registro en el log (y en el caso de producción se envía a una tabla kudu en la LZ)

        Parameters
        ----------
        exception : Exception, optional
            En el caso que haya un error en la ejecución se envia el error encontrado.

        Returns
        -------
        None.

        """
        fecha, hora = self.__currentDateTime()
        self.executionRecord["fecha_fin"] = int(fecha)
        self.executionRecord["hora_fin"] = hora
        self.executionRecord["duracion"] = round(time.time() - self.executionRecord["t1"], 2)

        self.executionRecord["estado_fin"] = "OK" if exception is None else "ERROR"
        self.executionRecord["texto_fin"] = "Correcto" if exception is None else str(exception).replace("\"", "\\\"")

        self.executionRecord.pop("t1")

        self.__saveRecord()

    def ejecutar(self):
        """Se encarga de ejecutar cada paso enviado en la lista.  Cada elemento de la lista (Step)
        tiene un método de ejecutar (definido por el usuario) el cual será ejecutado en orden.

        Raises
        ------
        Exception
            Cuando la lista no se encuentra inicializada o hay un error en alguno de los pasos.

        Returns
        -------
        None.

        """
        if not self.initialized:
            raise Exception("No se han inicializado los pasos del orquestador")

        # Necesario para conocer el nombre del paquete que me está llamando
        callerPath = "No_Encontrado" + os.sep + "No_Encontrado"
        try:
            found = False
            frame = sys._getframe(0)
            while not found:
                if frame.f_back is not None:
                    if frame.f_back.f_locals.get('__file__', None):
                        frame = frame.f_back
                        found = True
                    else:
                        frame = frame.f_back

                elif frame.f_locals.get('__file__', None):
                    found = True

            callerPath = frame.f_locals['__file__']
            # callerPath = sys._getframe(1).f_locals['__file__']
        except Exception as e:
            self.log.info("No se encontró el nombre del paquete, usando: {0}".format(callerPath))
            self.log.info("Mensaje búsqueda de paquete: {0}".format(e))

        self.__getPackageName(callerPath)
        self.__startExecRecord()

        fTic = time.time()
        try:
            tic = time.time()
            inicio = ""
            fin = ""
            duracion = 0
            step_name = ""

            payload = None

            self.log.info("Inicializando Ejecución de Orquestador")
            step_names = [type(sx).__name__ for sx in self.steps]
            tasks_ids = self.log.establecer_tareas("", "ORQUESTADOR", step_names)

            for i, sx in zip(tasks_ids, self.steps):
                inicio = self.__currentTime()
                step_name = type(sx).__name__
                self.log.iniciar_tarea(i)
                self.log.info("------------------------------------------------")

                self.log.info("Ejecutando Paso ** {0} **".format(step_name))

                mT = sx.getConfigValue("maxTries")

                tic = time.time()
                sx.setPayload(payload)
                self.reStarter(sx, maxTries=mT)
                payload = sx.getPayload()
                toc = time.time()

                fin = self.__currentTime()
                duracion = round(toc - tic, 2)

                self.log.info("Paso {0} finalizado en {1}s".format(step_name, duracion))
                self.__genStats(step_name, inicio, fin, duracion, "OK", "")
                self.log.finalizar_tarea(i)

            self.__endExecRecord()
            fToc = time.time()

            self.log.info("------------------------------------------------")
            self.log.info("Finalizó Orquestador Satisfactoriamente")
            self.log.info("Duración Total en {0}s".format(round(fToc - fTic, 2)))
            self.log.info("------------------------------------------------")


        except Exception as e:
            fToc = time.time()
            toc = time.time()
            self.log.error("------------ ERROR EN EJECUCION  ---------------------")
            self.log.error("El Orquestador ha encontrado un Error en el proceso")

            self.log_exception(e)

            fin = self.__currentTime()
            duracion = round(toc - tic, 2)

            self.__genStats(step_name, inicio, fin, duracion, "ERROR", "{}".format(e))
            self.__endExecRecord(e)
            self.log.info("Duración Total en {0}s".format(round(fToc - fTic, 2)))
            self.log.info("------------------------------------------------")

            raise e
