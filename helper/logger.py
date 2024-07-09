# -*- coding: future_fstrings -*-

from datetime import datetime, timedelta
from pyfiglet import figlet_format
from pathlib  import Path
import logging
import pandas        as pd
import numpy         as np
import re
import sys


class Logger:
    """
        Clase utilizada para administrar y visualizar los queries de Impala Helper

        Attributes
        ----------
        nombre : str
            Nombre del proyecto
    """

    def __init__(self, nombre='Impala Helper', log_level='INFO', path='./logs/', log_type=''):
        """
            Parameters
            ----------
            nombre : str
                Nombre del proyecto

            log_level : str
                Nivel de informacion que sera mostrada

            path : str
                Ruta en la cual se desea guardar los archivos de log

            log_type: str
                Tipo de log a generar
        """
        nombre         = re.sub('\s+',' ',nombre.strip()).upper()
        self.pd_version = int(pd.__version__.replace(".",""))
        self.nombre    = nombre
        self.fullFilePath = None

        #------------------------------------------------------------------------------------------
        self.log, self.console_hand = self._manage_logs(path, log_level, log_type)
        #------------------------------------------------------------------------------------------

        columnas    = ['i','n','documento','tipo','nombre','estado','hora_inicio','duracion', 'consulta']
        self.df                = pd.DataFrame(columns=columnas)
        self.df['hora_inicio'] = pd.Series(dtype='datetime64[ns]')
        self.df['duracion']    = pd.Series(dtype='timedelta64[ns]')

        # Tamaños minimos establecidos para las columnas que se actualiza a los requeridos
        self.widths = pd.Series({'i':1,'tipo':4,'nombre':5,'estado':12,'hora_inicio':13,'duracion':11})

        # Total de acciones realizadas
        self.n = [0]
        

    def set_log_level(self, log_level):
        """
        Metodo utilizado para redefinir el nivel de informacion mostrado

        Parameters
        ----------
        log_level : int
            Nuevo nivel del log
        """
        self.log.setLevel(log_level)
        self.log_level = self.log.level

    # Metodos para administrar printeos -----------------------------------------------------------
    def debug(self, *args, **kargs):
        """
            Metodo para guardar y mostrar un mensaje con relevancia DEBUG
        """
        self.log.addHandler(self.console_hand)
        self.log.debug(*args, **kargs)
        self.log.removeHandler(self.console_hand)

    def info(self, *args, **kargs):
        """
            Metodo para guardar y mostrar un mensaje con relevancia INFO
        """
        self.log.addHandler(self.console_hand)
        self.log.info(*args, **kargs)
        self.log.removeHandler(self.console_hand)

    def _info(self, *args, **kargs):
        """
            Metodo privado para guardar y mostrar un mensaje con relevancia INFO,
            se diferencia del publico en que se usa para poder sobreescribirlo en 
            el orquestador
        """
        self.info(*args, **kargs)

    def warning(self, *args, **kargs):
        """
            Metodo para guardar y mostrar un mensaje con relevancia WARNING
        """
        self.log.addHandler(self.console_hand)
        self.log.warning(*args, **kargs)
        self.log.removeHandler(self.console_hand)

    def error(self, *args, **kargs):
        """
            Metodo para guardar y mostrar un mensaje con relevancia ERROR
        """
        self.log.addHandler(self.console_hand)
        self.log.error(*args, **kargs)
        self.log.removeHandler(self.console_hand)

    def exception(self, *args, **kargs):
        """
            Metodo para guardar y mostrar un mensaje con relevancia ERROR
        """
        self.log.addHandler(self.console_hand)
        self.log.exception(*args, **kargs)
        self.log.removeHandler(self.console_hand)

    def critical(self, *args, **kargs):
        """
            Metodo para guardar y mostrar un mensaje con relevancia CRITICAL
        """
        self.log.addHandler(self.console_hand)
        self.log.critical(*args, **kargs)
        self.log.removeHandler(self.console_hand)

    # Metodos publicos para actualizar el estado de un query --------------------------------------
    def inicia_consulta(self, query_id):
        """
        Inicia el cronometro y reporta el estado del query identificado

        Parameters
        ----------
        query_id : int
            Identificacion del query a iniciar
        """
        self.log.info(re.sub(r'\n',r'\n            ',f'Inicia la ejecucion de la siguiente consulta:\n{self.df.loc[query_id,"consulta"]}'))
        start = datetime.now()
        self._reportar(query_id,estado='ejecutando',hora_inicio=start)

    def diagnostico_consulta(self, diagnostico):
        """
        Inicia el cronometro y reporta el estado del query identificado

        Parameters
        ----------
        diagnostico : str
            Mensaje a imprimir en el archivo log
        """
        self.log.info(re.sub(r'\n',r'\n            ',f'DIAGNOSTICO >>> {diagnostico}'))

    def error_query(self, query_id, exception, con_resumen=True):
        """
        Para el cronometro, y reporta el estado de todo el proyecto hasta el error

        Parameters
        ----------
        query_id : int
            identificacion del query a reportar como errado
        """
        txt = exception.args[1] if len(exception.args)>=2 else str(exception)
        txt1 = ('\n'+txt).replace('\n','\n            ')
        self.log.error(txt1)
        duracion = datetime.now() - self.df.loc[query_id,'hora_inicio']
        self._reportar(query_id, estado='error',duracion=duracion)
        if self._check_print():
            if con_resumen:
                doc = self.df.loc[query_id,'documento']
                for i in self.df.loc[query_id:].index:
                    if self.df.loc[i,'documento'] != doc:
                        doc = self.df.loc[i,'documento']
                        self.print(self._encabezado(doc))
                    self._reportar(i,end='\n')
                self.finalizar(groupby=['documento','estado'])
            else:
                self.print('\n'+self._line())
            self.print()
            self.print(self._line())
            self.print('Consulta que fallo:\n')
            self.print(self.df.loc[query_id,'consulta'])
            self.print(self._line())
            self.print()
            self.print(self._line())
            self.print(txt)
            self.print(self._line())

    def finaliza_consulta(self, query_id, end='\n', mensaje='Finalizó la ejecucion del query'):
        """
        Finaliza y reporta un mensaje en el archivo de logs

        Parameters
        ----------
        query_id : int
            identificacion del query a reportar como finalizado

        end : Optional str
            String pegado al final del reporte hecho, usado para reemplazar
            o generar una nueva linea

        mensaje : Optional str
            Mensaje que se grabara en el archivo de logs para identificar mejor
            que es lo que se finaliza
       """
        duracion = datetime.now() - self.df.loc[query_id,'hora_inicio']
        self.log.info(f'{mensaje}, duracion: {self.formatter(duracion)}')
        self._reportar(query_id, estado='finalizado',duracion=duracion,end=end)

    def finalizar(self, groupby='documento'):
        """
        Metodo usado para finalizar el reporte de los datos y mostrar un resumen del plan de ejecucion

        Parameters
        ----------
        groupby : Optional str or list[str]
            Llaves por las cuales se generara el resumen
        """
        text = (
            f'{self._line()}\n\n'
            'Resumen de consultas:\n'
            f'{self._resumen(groupby)}\n'
        )
        self.print(text)

    def _finalizar(self, groupby='documento'):
        '''
        Metodo privado usado para finalizar el reporte de los datos, se diferencia con el metodo publico
        en que se puede sobreescribir para evitarlo en el orquestador

        Parameters
        ----------
        groupby : Optional str or list[str]
            Llaves por las cuales se generara el resumen
        '''
        self.finalizar(groupby)

    # Metodos publicos para mostrar resultados ----------------------------------------------------
    def imprimir_plan(self):
        """
            Metodo publico para poder imprimir todo el plan de ejecucion seguido por el
            modulo de Impala Helper
        """
        if not self.df.empty:
            doc  = None
            lens = self.widths
            for i in self.df.index:
                if self.df.loc[i,'documento'] != doc:
                    doc = self.df.loc[i,'documento']
                    print(self._encabezado(doc))
                vals = self.df.loc[i,self.columnas]
                print('\r ' + ' '.join([f'{self.formatter(vals[i]):>{lens[i]}}' for i in self.columnas]) + " ")
            print(self._line())

    def imprimir_resumen(self, groupby=['documento','estado']):
        """
            Metodo publico util para mostrar de manera resumida el plan y el estado
            de ejecucion realizado por Impala Helper

            Parameters
            ----------
            groupby : str or list[str]
                Llaves por las cuales se va a agrupar y mostrar el resumen. Las llaves
                permitidas son: [`documento`,`estado`,`tipo`,`nombre`]
        """
        if not self.df.empty:
            print(self._resumen(groupby))

    # Metodos para establecer queries y determinar su tipo y la tabla afectada --------------------
    def _get_type(self, consulta):
        """
        Metodo privado para determinar que tipo de consulta es la que se desea hacer

        Parameters
        ----------
        consulta : str
            texto que contiene la consulta a ser validada

        Returns
        -------
        tipo : str
            tipo de la consulta
        """
        consulta = consulta.lstrip()
        tipo = consulta.split(None,1)[0].upper()
        
        if tipo in self.reg_exp:
            return tipo
        if tipo == 'WITH' and re.search(self.reg_exp['INSERT'], consulta, re.IGNORECASE):
            return 'INSERT'
        if tipo == 'WITH' or tipo == 'SELECT':
            return 'SELECT'
        if tipo == 'SET':
            # SET PARQUET_FILE_SIZE=512m;
            # SET Num_Nodes=1;
            # SET SYNC_DDL=1;
            return 'SET_OPTION'
        if tipo == 'EXPLAIN':
            return 'EXPLAIN'
        return 'DESCONOCIDO'

    def _get_table(self, consulta, tipo):
        """
            Metodo privado para determinar cual es la tabla afectada por la consulta

            Parameters
            ----------
            consulta : str
                texto que contiene la consulta a ser validada

            tipo : str
                tipo de la consulta a ser validada
        """
        if tipo in self.reg_exp:
            match = re.search(self.reg_exp[tipo],consulta,re.IGNORECASE)
            if match:
                return match.group('tabla')
        return ''

    def establecer_tareas(self, documento="", tipo="", nombre="", consulta=""):
        """
        Método usado para establecer actividades dentro del logger en el 
        plan de ejecucion

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
            indice que identifica a cada una de las consultas o actividades especificadas
        """
        try:
            df = pd.DataFrame({
                'documento'   : documento,
                'tipo'        : tipo,
                'nombre'      : nombre,
                'consulta'    : consulta,
                'estado'      : 'pendiente',
            })
        except ValueError:
            df = pd.DataFrame({
                'documento'   : documento,
                'tipo'        : tipo,
                'nombre'      : nombre,
                'consulta'    : consulta,
                'estado'      : 'pendiente',
            }, index=[0])
        
        df['n']      = df.index.map(lambda x: x+1+self.df.shape[0])
        self.n[0] = len(self.df)+df.shape[0]
        df['i']      = df['n'].apply(lambda x: self.I(x, self.n))
        #self.df      = self.df.append(df, sort=False, ignore_index=True)
        self.df      = pd.concat([self.df , df] , axis = 0 , sort=False, ignore_index=True )
        index = self.df.index[-df.shape[0]:] #Retorna los indices en los que quedaron las consultas
        self.widths  = self._get_widths(index) #Evaluar todo el dataframe
        self.log.info(f'Se agregaron {len(index):,} consultas al plan de ejecucion')
        return index #Retorna los indices en los que quedaron las actividades

    def establecer_queries(self, documento, lista_consultas):
        """
        Metodo usado para establecer los queries y el plan de ejecucion.

        Parameters
        ----------
        documento : str or list[str]
            ruta o nombre del documento al cual pertenece el query

        lista_consultas : list
            Lista de cada una de las consultas que se planean ejecutar

        Returns
        -------
        index : pandas.Index
            indice que identifica a cada una de las consultas en `lista_consultas`
        """
        documento = documento if documento is not None else ''

        tipos  = [self._get_type(x)    for x   in lista_consultas]
        tables = [self._get_table(x,y) for x,y in zip(lista_consultas,tipos)]
        index  = self.establecer_tareas(documento,tipos,tables,lista_consultas)
        return index #Retorna los indices en los que quedaron las consultas
    
    def _modificar_tipo(self, query_id, nuevo_tipo):
        """
        Método privado utilizado para modificar el tipo de query por si es necesario
        
        Parameters
        ----------
        query_id : int
            Número que identifica el query a ser modificado
        
        nuevo_tipo : str
            Nuevo tipo que le será asignado al query
        """
        self.df.loc[query_id,'tipo'] = nuevo_tipo
        self._get_widths(query_id)
    
    # Metodo interno para actualizar y mostrar valores actualizados -------------------------------
    def _reportar(self, query_id, estado=None, hora_inicio=None, duracion=None, end=''):
        """
            Metodo privado usado para actualizar el estado de un query y reportarlo

            Parameters
            ----------
            query_id : int
                identificacion del query a reportar

            estado : Optional str
                nuevo estado del query

            hora_inicio : Optional datetime
                Hora en la que empieza el query

            duracion : Optional timedelta
                Duracion total de ejecucion del query

            end : Optional str
                String pegado al final del reporte hecho, usado para reemplazar
                o generar una nueva linea
        """
        if estado     : self.df.loc[query_id,     'estado'] =      estado
        if hora_inicio: self.df.loc[query_id,'hora_inicio'] = hora_inicio
        if duracion   : self.df.loc[query_id,   'duracion'] =    duracion

        # self.df.loc[query_id, 'i'] = f'{query_id+1}/{self.df.shape[0]}'
        vals     = self.df.loc[query_id,self.columnas]
        lens     = self._get_widths(query_id)
        s = ' '.join([f'{self.formatter(vals[i]):>{lens[i]}}' for i in self.columnas])
        self.print(f'\r {s} ', end=end)

    def print(self, *args, **kargs):
        """
            Metodo interno para imprimir verificando el nivel de informacion
            especificado en `log_level`
        """
        if self._check_print():
            print(*args, **kargs)

    def _check_print(self):
        """
            Metodo interno para verificar el nivel de informacion nivel INFO
        """
        return self.log.isEnabledFor(self.INFO)

    # Metodos internos para dar formatos a titulos y valores --------------------------------------
    def _line(self):
        """
        Metodo interno que imprime una linea del tamaño adecuado a los datos
        """
        size = self.widths.sum() + (len(self.widths) + 1)
        return f'{"":->{size}}'

    def _resumen(self, groupby='documento'):
        """
        Metodo privado para mostrar un resumen del plan de ejecucion

        Parameters
        ----------
        groupby : Optional str or list[str]
            Llaves por las cuales se generara el resumen
        """
        res  = self.df.groupby(groupby, as_index=False, sort=False).agg({'i':'count','duracion':'sum'})
        res.rename(columns={'i':'total'}, inplace=True)
        if self.pd_version >= 210:
            lens = res.map(lambda x: len(self.formatter(x))).max()
        else:
            lens = res.applymap(lambda x: len(self.formatter(x))).max()
        mins = pd.Series({'documento':9,'total':6, 'duracion':11,'estado':8,'tipo':6,'nombre':5})
        lens = lens.combine(mins,max)[lens.index]
        k    = {'documento':'^','total':'>','duracion':'>','estado':'^','tipo':'^','nombre':'>'}
        size = lens.sum()+(len(lens)+1)
        nl = ' \n ' #fstrings no permite backslash, toca hacerlo por fuera

        info_resumen = nl.join([
                " ".join([
                    f"{self.formatter(j[i]):>{int(lens[i])}}" for i in res.columns
                ])
            for _,j in res.iterrows()])

        text = (
            f'{"":->{size}}\n' + " ".join([f"{i:{k.get(i,'^')}{int(lens[i])}}" for i in res.columns]) + ' \n'
            f'{"":->{size}}\n'
            f''' {info_resumen} \n'''            #Registros
            f'{"":->{size}}'    #Linea separadora
        )
        if res.shape[0]>1:
            total = res.apply(lambda x: x.nunique() if x.name in groupby else x.sum())
            text1 = (
                "\n " + ' '.join([f'{self.formatter(total[i]):>{int(lens[i])}}' for i in res.columns]) + " \n"
                f'{"":->{size}}'
            )
        else:
            text1 = ''
        return text + text1

    def _titulo(self, titulo, max_length=None):
        """
        Metodo privado util para imprimir titulos centrados del tamanio adecuado

        Parameters
        ----------
        titulo : str
            comentario o titulo a reportar

        max_length : int
            tamaño maximo permitido para el comentario
        """
        size = self.widths.sum() + (len(self.widths) + 1)
        max_length = max_length if max_length else size - 4
        return f'{self.formatter(titulo,max_length):^{size}}'

    def _columnas(self):
        """
        Metodo privado para reportar los titulos de las columnas
        """
        lens = self.widths
        return ' ' + ' '.join([f'{i:^{lens[i]}}' for i in self.columnas]) + ' '

    def _encabezado(self, documento=None):
        """
        Reporta un encabezado con titulo del documento y nombre de las columnas

        Parameters
        ----------
        documento : str
            Nombre del documento al cual se le desea reportar el encabezado
        """
        n = '\n'
        text = (
            f'{self._line()+n            if documento else ""}'
            f'{self._titulo(documento)+n if documento else ""}'
            f'{self._line()}\n'
            f'{self._columnas()}\n'
            f'{self._line()}'
        )
        return text

    def _get_widths(self, queries_id=None):
        """
        Metodo privado que calcula el tamaño apropiado para las columnas a presentar

        queries_id : Optional int
            Identificacion del query al que se le realizara el calculo del tamanio, si no
            se especifica, se realizara el calculo sobre todo el plan de ejecucion
        """
        if queries_id is None:
            if self.pd_version < 210:
                lens = self.df[self.columnas].applymap(lambda x: len(self.formatter(x))).max()
            else:
                lens = self.df[self.columnas].map(lambda x: len(self.formatter(x))).max()
        else:
            df        = self.df.loc[queries_id,self.columnas]
            df        = df if type(df) is pd.DataFrame else df.to_frame().T
            if self.pd_version >= 210:
                lens      = df.map(lambda x: len(self.formatter(x))).max()
            else:
                lens = df.applymap(lambda x: len(self.formatter(x))).max()
        self.widths = self.widths.combine(lens, max)
        return self.widths

    def print_encabezado(self, documento=None):
        """
            Metodo utilizado para imprimir un encabezado incluyendo el nombre del documento

            Parameters
            ----------
            documento : str
                Nombre del documento que se quiere aparezca en el encabezado
        """
        self.print(self._encabezado(documento))

    def _print_encabezado(self, documento=None):
        '''
            Metodo privado para imprimir un encabezado incluyendo el nombre del documento.
            Este metodo se diferencia del publico en que se puede sobreescribir en el orquestador
            para evitar que aparezcan los titulos 

            Parameters
            ----------
            documento : str
                Nombre del documento que se quiere aparezca en el encabezado
        '''
        self.print_encabezado(documento)

    def _print_line(self):
        '''
            Metodo privado para imprimir una linea. Este metodo se usará para poder sobreescribirlo
            en el orquestador y evitar que aparezca cuando no deberia
        '''
        self.print(self._line())

    # Formatos para imprimir objetos --------------------------------------------------------------
    def format_delta(self, td):
        """
        Metodo usado para formatear duraciones de tiempo

        Parameters
        ----------
        td : timedelta
            Objeto a ser formateado
        """
        mm, ss = divmod(td.seconds, 60)
        hh, mm = divmod(mm, 60)
        s = f'{mm:02}:{ss:02}.{int(td.microseconds/100000)}'
        if hh:
            s = f'{hh}:{s}'
        if td.days:
            s = f"{td.days} day{'s' if abs(td.days)>1 else ''}, {'0:' if not hh else ''}{s}"
        return s

    def format_time(self, tm):
        """
        Metodo usado para formatear horas

        Parameters
        ----------
        tm : timedelta
            Objeto a ser formateado
        """
        return tm.strftime('%I:%M:%S %p')

    def formatter(self, value, max_length=None):
        """
            Metodo usado para formatear cualquier tipo de dato

            Parameters
            ----------
            value : any
                Objeto a ser formateado

            max_length : int
                tamaño maximo que se permitira
        """
        max_length = max_length if max_length is not None else self.max_length
        trunc = lambda x,y: x if len(x)<=y else '...'+x[-y+3:]
        if type(value) in [datetime, pd.Timestamp]:
            return self.format_time(value)
        if type(value) is np.datetime64 and pd.notnull(value):
            return self.format_time(pd.Timestamp(value))
        if type(value) in [timedelta, pd.Timedelta]:
            return self.format_delta(value)
        if type(value) is np.timedelta64 and pd.notnull(value):
            return self.format_delta(pd.Timedelta(value))
        return trunc(str(value),max_length) if pd.notnull(value) else ''

    # Manejo interno de los archivos de Logs ------------------------------------------------------
    def _manage_logs(self, path, log_level, log_type):
        """
            Metodo interno para generar un log y configurarlo
        """
        folder = Path(path)
        log_prefix = ''

        log = logging.getLogger('prueba')
        log.setLevel(log_level)
        self.log_level = log.level

        if log.isEnabledFor(self.INFO):
            print(figlet_format(self.nombre.replace(' ','\n')))

        if log_type != '':
            log_prefix = log_type + '_'

        nombre_log = folder / f"{log_prefix}{datetime.now().strftime('%Y%m%d_%H%M%S')}_{self.nombre.replace(' ','_').lower()}.log"
        self.fullFilePath = str(nombre_log)
        formatter = logging.Formatter('%(asctime)s - [%(levelname)s] - %(message)s','%Y-%m-%d %H:%M:%S')
        
        console_hand = logging.StreamHandler()
        console_hand.setFormatter(formatter)
        
        if folder.exists() and folder.is_dir():
            file_hand = logging.FileHandler(str(nombre_log), encoding='utf-8')
            file_hand.setFormatter(formatter)
            log.addHandler(file_hand)
            log.info('--------------------------------------------------')
            log.info(self.nombre)
            log.info('--------------------------------------------------')
            log.addHandler(console_hand)
            log.info(f'Guardado logs en: {nombre_log.absolute()}')
            log.removeHandler(console_hand)
        else:
            null_hand = logging.NullHandler()
            log.addHandler(null_hand)
            log.addHandler(console_hand)
            log.warning(f'No se encontro la carpeta "{folder.absolute()}" para guardar los logs')
            log.removeHandler(console_hand)


        return log, console_hand
        #------------------------------------------------------------------------------------------

    # Variables internas --------------------------------------------------------------------------
    
    # Niveles de logging
    NOTSET   = logging.NOTSET
    DEBUG    = logging.DEBUG
    INFO     = logging.INFO
    WARNING  = logging.WARNING
    ERROR    = logging.ERROR
    CRITICAL = logging.CRITICAL
    
    # Nombre de las columnas que se van a reportar
    columnas = ['i','tipo','nombre','estado','hora_inicio','duracion']

    # Tamanio maximo permitido por campo
    max_length = 40

    # Expresiones regulares usadas para poder identificar la tabla afectada por cada
    # tipo de query usado
    reg_exp = {x:y.replace(' ',r'\s+') for x,y in
        {
            'ALTER'      : r'alter (table|view) (?P<tabla>(\w+\.)?\w+)',
            'COMPUTE'    : r'compute (incremental )?stats (?P<tabla>(\w+\.)?\w+)',
            'CREATE'     : r'create (external )?(table|view) (if not exists )?(?P<tabla>(\w+\.)?\w+)',
            'DROP'       : r'drop (table|view|(incremental )?stats) (if exists )?(?P<tabla>(\w+\.)?\w+)',
            'INSERT'     : r'insert (into|overwrite) (table )?(?P<tabla>(\w+\.)?\w+)',
            'INVALIDATE' : r'invalidate metadata (?P<tabla>(\w+\.)?\w+)',
            'REFRESH'    : r'refresh (?P<tabla>(\w+\.)?\w+)',
            'TRUNCATE'   : r'truncate (table )?(if exists )?(?P<tabla>(\w+\.)?\w+)',
            'USE'        : r'use (?P<tabla>\w+)',
            'DESCRIBE'   : r'describe (formatted )?(?P<tabla>(\w+\.)?\w+)',
            'SHOW'       : r'show (create table|create view|table stats|column stats|partitions|files in) (?P<tabla>(\w+\.)?\w+)',
            'SET_OPTION' : r'set (sync_ddl|num_nodes|parquet_file_size)?(?P<tabla>(\w+\=)?\w+)'
        }.items()
    }
    
    class I:
        def __init__(self, i, total):
            self.i     = i
            self.total = total
        
        def __str__(self):
            return f'{self.i}/{self.total[0]}'
        
        def __repr__(self):
            return self

