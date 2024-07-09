# -*- coding: future_fstrings -*-

from termcolor     import cprint
from pyfiglet      import figlet_format
from datetime      import datetime, timedelta

import sys

from helper.logger import Logger

import pandas      as pd
import re

class Logger(Logger):
    """
    Clase utilizada para administrar y visualizar las tareas realizadas por el orquestador
    
    Attributes
    ----------
    nombre : str
        Nombre del proyecto
    """
    
    id_actual = None
    
    def __init__(self, nombre='Orquestador', log_level='INFO', path='./logs/', log_type = ''):
        """
        Parameters
        ----------
        nombre : str
            Nombre del proyecto
        
        log_level : str
            Nivel de informacion que sera mostrada
        
        path : str
            Ruta en la cual se desea guardar los archivos de log

        log_type : str
            Tipo de log a generar [compilación, estabilidad, normal]
        """
        super().__init__(nombre=nombre, log_level=log_level, path=path, log_type = log_type)
        self.df['etapa'] = None
        self.df['sub_i'] = None
        self.widths['i'] = 6
    
    def establecer_tareas(self, documento="", tipo="", nombre="", consulta=""):
        """
        Metodo usado para establecer las tareas en el plan de ejecucion.
        
        Parameters
        ----------
        documento : str or list[str]
            ruta o nombre del documento al cual pertenece la actividad

        tipo : str or list[str]
            tipo que especifica la clase de actividad a realizar
        
        nombre : str or list[str]
            nombre que identifica la tabla o la actividad en particular a realizar

        consulta : str or list[str]
            consulta o actividad a realizar

        Returns
        -------
        index : pandas.Index
            indice que identifica a cada una de las consultas en `lista_consultas`
        """
        index = super().establecer_tareas(documento, tipo, nombre, consulta)
        if self.id_actual:
            n = self.df.loc[self.id_actual,'n']
            sub_tareas_actuales = self.df.query('n==@n').shape[0]
            self.df.loc[index,'sub_i'] = list(range(sub_tareas_actuales,sub_tareas_actuales+len(index),1))
            self.df.loc[index,'n']     = n
            self.df.loc[index,'i']     = self.df.loc[index].apply(
                lambda x: f"{int(x['n'])}-{int(x['sub_i'])}/{self.df['n'].nunique()}", axis=1
            )
        else:
            self.df.loc[index,'sub_i'] = 0
        return index
    
    def iniciar_tarea(self, task_id):
        """
        Inicia el cronometro y reporta el estado de la tarea identificada
        
        Parameters
        ----------
        task_id : int
            Identificacion de la tarea a iniciar
        """
        self.log.info(re.sub(r'\n',r'\n            ',f'Inicia la ejecucion de la tarea {self.df.loc[task_id,"nombre"]}'))
        start = datetime.now()
        self._reportar(task_id,estado='ejecutando',hora_inicio=start)
        self.id_actual = task_id
    
    def actualizar(self, estado=None, duracion=None):
        """
        Metodo publico usado para actualizar el estado de una tarea y reportarlo
        
        Parameters
        ----------
        estado : Optional str
            nuevo estado de la tarea
        
        duracion : Optional timedelta
            Duracion total de ejecucion de la tarea
        """
        if self.id_actual is not None:
            self._reportar(self.id_actual, estado, duracion)
    
    def finalizar_tarea(self, task_id):
        """
        Finaliza y reporta un mensaje en el archivo de logs
        
        Parameters
        ----------
        task_id : int
            identificacion del query a reportar como finalizado
       """
        duracion = datetime.now() - self.df.loc[task_id,'hora_inicio']
        self.log.info(f'Finalizó la ejecucion de la tarea {self.df.loc[task_id,"nombre"]}, duracion: {self.formatter(duracion)}')
        self._reportar(task_id, estado='finalizado',duracion=duracion,end='\n')
        self.id_actual = None
    
    def _print_encabezado(self, documento=None):
        '''
        Metodo privado para imprimir un encabezado incluyendo el nombre del documento.
        Este metodo se usa para evitar printeos indeseados de los metodos publicos
        del helper
        
        Parameters
        ----------
        documento : str
            Nombre del documento que se quiere aparezca en el encabezado
        '''
        pass #self.print('\r',end='')
    
    def _print_line(self):
        '''
        Metodo privado para imprimir una linea. Este metodo se usa para evitar printeos
        indeseados de los metodos publicos del helper
        '''
        pass
    
    def _finalizar(self, groupby='documento'):
        '''
        Metodo privado usado para finalizar el reporte de los datos. Este metodo se usa
        para evitar printeos indeseados de los metodos publicos del helper
    
        Parameters
        ----------
        groupby : Optional str or list[str]
            Llaves por las cuales se generara el resumen
        '''
        pass
    
    def _info(self, *args, **kargs):
        """
        Metodo privado para guardar y mostrar un mensaje con relevancia INFO.
        Este metodo se usa para evitar printeos indeseados de los metodos publicos
        del helper
        """
        self.log.info(*args, **kargs)
    
