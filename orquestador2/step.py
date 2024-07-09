# -*- coding: utf-8 -*-
"""
Created on Wed Aug 18 11:38:44 2021

@author: RLARIOS
"""
from abc import ABC, abstractmethod
#import pkg_resources
import os
import json
import time
#import inspect
import importlib


class Step(ABC):
    """Clase que define los metodos básicos y abstractos para ser ejecutados por el orquestador quien herede 
    de esta clase deberá implementar el método de ejecutar
    """
    
    def __init__(self , **kwargs):
        """
        Constructor de la clase define las variables propias y las inicializa en Nulo
        dado que el Orquestador es el que las define en su inicialización

        Returns
        -------
        None.

        """
        self.kwa = kwargs
        self.payload = None
        
        self.helper = None
        self.sparky = None
        self.emailer = None
        self.log = None
        self.config = {}
        self.globalConfig = None
        
    def verifyIamAStep(self):
        """Clase de ayuda para verificación

        Returns
        -------
        Str
            Nombre de la clase.

        """
        return type(self).__name__
        
    def setModules(self , helper , sparky , log , emailer ):
        """Define los elementos de ayuda (helpers) para la conexión a la plataforma analitica
        Estos son enviados por el Orquestador.  Así mismo se configura así mismo con el archivo de configuración

        Parameters
        ----------
        helper : Helper
            Impala Helper.
        sparky : Sparky
            Sparky.
        log : Logger
            Logger.

        Returns
        -------
        None.

        """
        self.helper , self.sparky , self.log , self.emailer = helper , sparky , log , emailer
        self.setConfiguration()
        
    def setConfiguration(self):
        """Lee el archivo de configuración ubicado en {FilePath}/static/config.json

        Returns
        -------
        None.

        """
        path = self.getFolderPath() + "config.json"
        
        if os.path.exists(path):
            with open( path ) as f_in :
                json_str = f_in.read()
            
            cfg = json.loads( json_str )
            
            self.config = cfg.get(type(self).__name__,{})
            
            for k,v in self.kwa.items():
                #repisa los valores staticos por los enviados si estos son enviados
                self.config[k] = v
            
        
    def initGlobalConfiguration(self):
        """Ayuda al orquestador a traer la configuración Global inicial

        Returns
        -------
        Dict
            Diccionario si encuentra la llave "global", de lo contrario None.

        """
        path = self.getFolderPath() + "config.json"
        cfg = {}
        if os.path.exists(path):
            with open( path ) as f_in :
                json_str = f_in.read()
            
            cfg = json.loads( json_str )
            
        return cfg.get("global",None)
    
    def setGlobalConfig(self , cfg):
        """Una vez la configuración global se ha organizado por el orquestador, se repisa en cada paso
        """
        
        self.globalConfig = cfg
        
    def getGlobalConfiguration(self):
        """Retorna la configuración global tratada por el orquestador

        Returns
        -------
        Dict
            La configuración Global

        """
        return self.globalConfig
        
    
    def getConfigValue(self , name , default = None):
        """Devuelve un valor de la configuración

        Parameters
        ----------
        name : Str
            Nombre de la propiedad a traer.
        default : Valor por defecto a traer, optional
            DESCRIPTION. The default is None.

        Returns
        -------
        Valor de la configuración

        """
        return self.config.get(name,default)        
            
        
    def getHelper(self):
        """ Retorna el Impala Helper

        Returns
        -------
        Helper
            Impala Helper.

        """
        return self.helper
    
    def getSparky(self):
        """
        Devuelve el Sparky inicializado por el coordinadoe

        Raises
        ------
        Exception
            Cuando no se inicializó el Sparky desde el coordinador.

        Returns
        -------
        Sparky
            Módulo de Sparky inicializado.

        """
        if self.sparky is None:
            raise Exception("El coordinador se configuró para no utilizar Sparky, Está solicitándolo sin haberse inicializado")
            
        return self.sparky
    
    def getLog(self):
        """ Retorna el Logger

        Returns
        -------
        Logger
            logger.

        """
        return self.log
    
    def getStepConfig(self):
        """ Retorna la configuración específica del paso

        Returns
        -------
        Dict
            Configuración del paso.

        """
        return self.config    
    
    def setPayload(self , objeto):
        """Define el payload (valor o elemento para ser utiliado por otros pasos) este es utilizado por
        el orquestador antes de ejecutar el paso o es utilizado por el paso actual para entregar valores
        al paso siguiente

        Parameters
        ----------
        objeto : Any
            Cualquier tipo que se desee definir como Payload.

        Returns
        -------
        None.

        """
        self.payload = objeto
        
    def getPayload(self):
        """Extrae el payload (valor o elemento para ser utiliado por otros pasos) este es utilizado por
        el orquestador despues de ejecutar el paso o es utilizado por el paso actual para recibir valores del paso anterior

        Returns
        -------
        Any
            Cualquier tipo que se desee definir como Payload.

        """
        return self.payload   
    
    def getFolderPath(self):
        """Retorna el path donde se encuentra el archivo donde se definió esta clase"

        Returns
        -------
        pathStatic : Str
            Ruta donde se encuentra definida la clase.

        """
        #filename   = inspect.getframeinfo(inspect.currentframe()).filename
        #pathBase   = os.path.dirname(os.path.abspath(filename))
        #pathStatic = pathBase + os.sep + 'static/' 
        #return pkg_resources.resource_filename(type(self).__name__, 'static/')
        
        locMod = importlib.import_module(self.__module__)
        filename = locMod.__file__
        pathBase   = os.path.dirname(os.path.abspath(filename))
        pathStatic = pathBase + os.sep + 'static/'         
        
        return pathStatic
    
    def getSQLPath(self):
        return self.getFolderPath() + "sql/"
    
    def getModelPath(self):
        return self.getFolderPath() + "model/"
    
    def sendEmail(self , sender, receivers, subject, message , files = None ):
        self.emailer.send_email(sender, receivers, subject, message , files )
    
    
#%%  Helper Functions


    def executeFolder(self , folder , params = None, diagnostico = False):
        """Ejecuta todos los archivos SQL de un folder recorriendo recursivamente
        las carpetas dentro del folder.   Solo ejecuta archivos con extensión .sql.
        Recorre recursivamente primero las carpetas y luego los archivos dentro de ellas.
        

        Parameters
        ----------
        folder : Str
            Ruta del Directorio a ejecutar.
        params : Dict, optional
            Diccionario de parámetros a enviar para reemplazar parámetros en el helper. The default is None.
        diagnostico : boolean
            Booleano que define si la ejecución de un conjunto de sentencias SQLs se debe ejecutar con el
            diagnóstico de consumo de recursos

        Raises
        ------
        Exception
            Cuando hay Errores en la ruta enviada o en la ejecución del SQL.

        Returns
        -------
        None

        """

        def getOrder(folder):
            dirlist = sorted([x for x in os.listdir(folder) if os.path.isdir(os.path.join(folder, x))])
            filelist = sorted([x for x in os.listdir(folder) if not os.path.isdir(os.path.join(folder, x))])
            return dirlist + filelist
        
        def recorrer(folder ):
            if not os.path.isfile(folder):
                filesIn = getOrder(folder)
                for file in filesIn:
                    recorrer(folder + os.sep + file )
                 
            else:
                filename, file_extension = os.path.splitext(folder)
                if file_extension.lower() == ".sql":
                    self.log.info("Ejecutando archivo: {0}".format(folder))
                    self.helper.ejecutar_archivo(folder , params, diagnostico = diagnostico)
                    
        self.log.info("Ejecutando Directorio: {}".format(folder))

        if not os.path.isdir(folder):
            msg = "Problemas con executeFolder: Error en el directorio enviado.  No es un Directorio: " + folder
            self.log.error(msg)
            raise Exception(msg)
        else:
            try:
                tic = time.time()
                filesIn = getOrder(folder)
                for file in filesIn:
                    recorrer(folder + os.sep + file)
                    
                toc = time.time()
                msg = "Directorio ejecutado correctamente, duración(s): {}".format(round(toc-tic,2))
                self.log.info(msg)
            except Exception as e:
                self.log.error("Problemas con executeFolder!, Directorio: {0}, Error: {1} ".format( folder , e))
                raise
                
                
                
    def executeTasks(self , params = {}):
        """Ejecuta funciones de manera ordenada en la función

        Parameters
        ----------
        params : Dict, optional
            Diccionario de parámetros para enviar a cada una de las tareas

        Returns
        -------
        None.

        """
        tareas = self.getStepConfig().get("tareas" , [])
        
        try:
            
            if len(tareas) > 0:

                step_name = type(self).__name__
                tasks_ids = self.log.establecer_tareas("", step_name ,[x['nombre'] for x in tareas])
                
                for idx , tarea in zip(tasks_ids , tareas):
                    self.log.iniciar_tarea(idx)
                    #Cada tarea es un diccionario con nombre y kwargs
                    method = getattr(self, tarea['nombre'])
                    kwargs = {}
                    for kwarg in tarea['kwargs']:
                        kwargs[kwarg] = params[kwarg]
                    
                    method(**kwargs)
                    self.log.finalizar_tarea(idx)
            else:
                self.log.warning("Este paso no tiene tareas configuradas")
                
        except Exception as e:
            self.log.error("Error Ejecutando tareas del paso: {0}".format(e))
            raise e
            


#%%   
    
        
    
    @abstractmethod
    def ejecutar(self):
        """
        Definición abstracta.  Quien Herede de esta clase deberá definir este método

        Returns
        -------
        None.

        """
        pass