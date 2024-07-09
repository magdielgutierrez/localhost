# -*- coding: future_fstrings -*-

from logging import exception
import pkg_resources
import sys
import os
import pyodbc
import pandas as pd
import platform
import re
import sqlparse
import getpass
from datetime import datetime, timedelta
from .logger import Logger
from pathlib import Path
import time


class Helper:
    """
    Clase que ofrece un API de alto nivel para interatuar con Impala de la LZ.

    Attributes
    ----------
    conn : pyodbc.connection
        Conexión de pyodbc a impala de la LZ

    max_tries : int
        Número máximo de reintentos de consultas fallidas

    fetch_size : int
        Tamaño de batch al usar fetchmany

    logger : Logger, Opcional
        Objeto encargado de administrar y mostrar el avance del plan de ejecucion, si este objeto
        es administrado, los parámetros `log_level` y `log_path` serán ignorados

    log_level : int or str, default='INFO'
        Nivel de registro que se va a usar.

    log_path : str, Opcional. Default: './logs/'
        Ubicacion en la cual se guardarán los logs generados, si no existe, no se guardarán
    """

    def __init__(self, max_tries=3, fetch_size=10 ** 4, logger=None, log_level='INFO', log_path='./logs/', dsn=None,
                 username=None, password=None, **kwargs):
        """
        Parameters
        ----------

        max_tries : int
            Maximum number of tries for failed queries

        fetch_size : int
            Tamaño de batch al usar fetchmany()

        logger : Logger, Opcional
            Objeto encargado de administrar y mostrar el avance del plan de ejecucion, si este objeto
            es administrado, los parámetros `log_level` y `log_path` serán ignorados

        log_level : int or str, default='INFO'
            Nivel de registro que se va a usar.

        log_path : str, Opcional. Default: './logs/'
            Ubicacion en la cual se guardarán los logs generados, si no existe, no se guardarán

        dsn : str, Opcional
            DSN de la conexión ODBC local a usar, si no existe, se intentará conexión con usuario y contraseña o Kerberos

        username : str, Opcional
            nombre de usuario a usar en caso de que falle Kerberos al momento de conectarse a la LZ

        password : str, Opcional
            contraseña a usar en caso de que falle Kerberos al momento de conectarse a la LZ
            
        kwargs : parámetro para enviar diccionario como parámetros, la idea es hacer la instanciación future proof.
            Los valores enviados por este parámetro pueden ser :
                sleep_reintento : float -> define el tiempo en segundo entre un reintento y el siguiente en caso de fallo
                sync_ddl : boolean -> define si se ejecuta el comando de sync_ddl = 1
                porcentaje_limit : float -> porcentaje de información a procesar o limitar de las tablas
                                                 origen de zona de resultados o datos crudos para pruebas de depuración. Si
                                                 no se especifica se toman los datos completos para la ejecución
                
            list_tbl_porc : list -> Lista de tablas a las cuales se le va a calcular un porcentaje de los datos. Si no se 
                                    especifica y el porcentaje_limit es entre (0, 100) se calcula el porcentaje de datos para 
                                    todas las tablas.

                                    Nota: El parametro tambien puede recibir el valor "*" lo que indica que va a calcular un 
                                    porcentaje de datos para todas las tablas del archivo o consulta a ejecutar.                                
        """
        self.logger = logger if issubclass(type(logger), Logger) else Logger(log_level=log_level, path=log_path)
        
        self.dsn = dsn
        self.usuar = username
        self.passwd = password
        
        self.max_tries = max_tries
        self.fetch_size = fetch_size
        self.kwargs = kwargs
        self.sleep_reintento = self.kwargs.get("sleep_reintento", 0)
        self.porcentaje_limit = self.kwargs.get("porcentaje_limit", 100)
        
        self.refresh_time = self.kwargs.get("refresh_time", 1000)
        self.lastCon = 0
        
        self.ctrls = self.kwargs.get("ctrls", {})
        assert int(self.porcentaje_limit) > 0, "Parámetro porcentaje_limit debe ser un entero mayor a 0"
        self.__check_params_control(self.ctrls)
        self.list_tbl_porc = self.kwargs.get("list_tbl_porc", [])
        self.tables_proc = []
        
        try:
            self.conn = self._obtener_conexion(self.dsn, self.usuar, self.passwd)
        except Exception as e:
            self.logger.exception(e)
            raise

        def intentar(cn, tries=1, option=1):
            try:
                return cn.execute('SET SYNC_DDL={0}'.format(option))
            except Exception as e:
                if tries >= self.max_tries:
                    self.logger.exception(e)
                    raise
                self.logger.warning(f'Error al ejecutar SET SYNC_DDL={option}, iniciando intento n. {tries + 1}')
                tr = tries + 1
                return intentar(cn, tr, option=option)

        if self.kwargs.get("sync_ddl", False):
            intentar(self.conn)
        else:
            intentar(self.conn, option=0)
            
    
    def force_reconnect(self):
        """
        Método que permite reconectar a impala en el momento que se desee

        Returns
        -------
        None.

        """
        self.conn = self._obtener_conexion(self.dsn, self.usuar, self.passwd)        
        self.lastCon = int(round(time.time()))

    
    def __get_conn(self):
        """
        Método privado que permite identificar el tiempo que ha pasado entre conexiones pra regenerar una nueva conexión

        Raises
        ------
        Exception En el caso que haya problemas regenerando una nueva conexión.

        Returns
        -------
        pyodbc.conn

        """
        try:
            actual = int(round(time.time()))
            elapsed = (actual - self.lastCon)
            if self.lastCon == 0 or elapsed > self.refresh_time:
                self.logger.info("Transcurrido: {0}, Tiempo de Refresco = {1}".format(elapsed , self.refresh_time))
                self.conn = self._obtener_conexion(self.dsn, self.usuar, self.passwd)
                self.lastCon = int(round(time.time()))
                return self.conn
    
            else:
                self.lastCon = int(round(time.time()))
                return self.conn
            
        except Exception as e:
            self.logger.error("Problemas con get_conn: " + str(e))
            raise e
            

    def actualizar_configuracion(self, config):
        """
        Método que permite actualizar algunas configuraciones en tiempo de ejecución

        Parameters
        ----------
        config : DICT
            Diccionario con las configuraciones a actualizar.

        Returns
        -------
        None.

        """
        if type(config) != dict:
            self.logger.exception(
                Exception("Debe enviar un parámetro de tipo dict, tipo enviado: {0}".format(type(config))))

        self.max_tries = config.get("max_tries", self.max_tries)
        self.fetch_size = config.get("fetch_size", self.fetch_size)
        self.sleep_reintento = config.get("sleep_reintento", self.sleep_reintento)
        self.porcentaje_limit = config.get("porcentaje_limit", 100)
        self.ctrls = config.get("ctrls", {})
        self.list_tbl_porc = config.get("list_tbl_porc", [])

    def __set_params_control(self, params, type = "ctrl"):
        try:
            if params is not None and type == "ctrl":
                tables = list(self.ctrls.keys())[:]
                for table in tables:
                    self.ctrls[str(table).format(**params)] = self.ctrls.pop(table)
            elif type == "tbl":
                self.list_tbl_porc = [tbl.format(**params).lower().strip() for tbl in self.list_tbl_porc] if params else self.list_tbl_porc
        except Exception as e:
            raise Exception(f"Error con reemplazo de parámetros en diccionario de controles: {e}")

    def ejecutar_archivo(self, ruta, params=None, diagnostico=False):
        """
        Método público para ejecutar un archivo local de consultas parametrizadas
        de Impala

        Parameters
        ----------
        ruta : str
            Ruta local del archivo
        params : dict
            Diccionario de parámetros
        diagnostico : bool
            Booleano que activa el proceso de diagnostico de consultas
        """
        nombre = ruta.replace('\\', '/').split('/')[-1]
        index, consultas = self._obtener_consultas(ruta, params)
        self.__set_params_control(params, type="ctrl")
        self.__set_params_control(params, type="tbl")
        self.logger._print_encabezado(nombre)
        cursor = self.__get_conn().cursor()
        for i, consulta in zip(index, consultas):
            try:
                self.logger.inicia_consulta(i)
                self._ejecutar_consulta(cursor, i, consulta, diagnos=diagnostico)
                self.logger.finaliza_consulta(i)
            except Exception as e:
                self.logger.error_query(i, e, True)
                raise

        self.logger._finalizar()

    def ejecutar_consulta(self, consulta, params=None, diagnostico = False):
        """
        Método público para ejecutar una consulta de Impala

        Parameters
        ----------
        consulta : str
            Consulta a ejecutar, posiblemente parametrizada

        params : dict
            Diccionario de parámetros

        diagnostico : bool
            Booleano que activa el proceso de diagnostico de consultas

        Returns
        -------
        cursor: pyodbc.cursor
            Cursor con resultados
        """
        cursor = self.__get_conn().cursor()

        consulta = self._eliminar_comentarios(consulta)
        consulta = consulta.format(**params) if params else consulta
        self.__set_params_control(params, type="ctrl")
        self.__set_params_control(params, type="tbl")

        index = self.logger.establecer_queries('', [consulta])
        i = index[0]

        self.logger._print_encabezado()
        try:
            self.logger.inicia_consulta(i)
            self._ejecutar_consulta(cursor, i, consulta, diagnos= diagnostico)
            self.logger.finaliza_consulta(i)
        except Exception as e:
            self.logger.error_query(i, e, False)
            raise

        self.logger._print_line()

        return cursor

    def obtener_cursor(self):
        """
        Método público para obtener un cursor

        Returns
        ------
        cursor : pyodbc.cursor
        """
        cursor = self.__get_conn().cursor()
        return cursor

    def _obtener_filas(self, query_id, consulta, end='\n', diagnos=False):
        """
        Método privado para la obtención de los registro para una consulta realizada

        Parameters
        ----------
        query_id : int
            id del query dentro del logger de Impala Helper

        consulta : str
            Consulta a ejecutar

        diagnos : bool
            Booleano que activa el proceso de diagnostico de consultas

        Returns
        ------
        header : list[str]
            Encabezado con nombres de columnas

        res : list
            Lista con filas de respuesta a consulta

        consultando : timedelta
            Tiempo que se tarda la consulta en generar resultados

        descargando : timedelta
            Tiempo que se tarda python en descargar los resultados de la consulta
        """
        inicio_consulta = datetime.now()
        iterator = self._obtener_iterador(query_id, consulta, end='', diagnos=diagnos)
        consultando = datetime.now() - inicio_consulta

        inicio_descarga = datetime.now()

        # header = ','.join([column[0] for column in iterator.description])
        header = [column[0] for column in iterator.description]
        self.logger._reportar(query_id, estado='descargando')
        res = []
        i = 0;
        try:
            while True:
                records = iterator.fetchmany(self.fetch_size)
                i = i + len(records)
                if len(records) == 0:
                    break;
                # res = res + records
                res.extend(records)
        except Exception as e:
            self.logger.exception(e)
            raise
        descargando = datetime.now() - inicio_descarga
        # self.logger.finaliza_consulta(query_id, end=end, mensaje='Finaliza obtener las filas')
        return header, res, consultando, descargando

    def obtener_filas(self, consulta, params=None):
        """
        Método público para obtener filas de resultados de una consulta

        Parameters
        ----------
        consulta : str
            Consulta parametrizada
        params : dict
            Diccionario de parámetros de consulta

        Returns
        ------
        header : str
            Encabezado con nombres de columnas concatenadas por ','

        res : list
            Lista con filas de respuesta a consulta
        """
        consulta = self._eliminar_comentarios(consulta)
        consulta = consulta.format(**params) if params else consulta
        index = self.logger.establecer_queries('', [consulta])
        query_id = index[0]
        self.logger._modificar_tipo(query_id, 'FILAS')

        self.logger._print_encabezado()
        self.logger.inicia_consulta(query_id)
        header, filas, consultando, descargando = self._obtener_filas(query_id, consulta)
        self.logger.finaliza_consulta(query_id, mensaje='Finaliza obtener las filas')
        self.logger._print_line()

        l_cols = len(header)
        l_filas = len(filas)
        cons = self.logger.formatter(consultando)
        desc = self.logger.formatter(descargando)
        self.logger._info(f'{l_filas:,} filas, {l_cols:,} columnas, {cons} consultando, {desc} descargando')

        return ','.join(header), filas

    def hacia_csv(self, consulta, ruta, params=None, sepa=",", encoding="utf-8"):
        """
        Método público para ejecutar una consulta y guardar los resultados en un
        archivo csv

        Parameters
        ----------
        consulta : str
            Consulta parametrizada
        params : str
            Diccionario de parámetros de consulta
        ruta : str
            Diccionario de parámetros de consulta
        sepa : str
            Caracter de separación de archivo
        encoding : str
            Encoding de archivo
        """
        consulta = self._eliminar_comentarios(consulta)
        consulta = consulta.format(**params) if params else consulta
        index = self.logger.establecer_queries('', [consulta])
        query_id = index[0]
        self.logger._modificar_tipo(query_id, 'A CSV')

        self.logger._print_encabezado()
        self.logger.inicia_consulta(query_id)

        path = Path(ruta)
        df, consultando, descargando, convirtiendo = self._obtener_dataframe(query_id, consulta, end='')
        inicio_guardado = datetime.now()
        self.logger._reportar(query_id, estado='guardando')
        df.to_csv(path, sep=sepa, encoding=encoding, index=False)
        guardando = datetime.now() - inicio_guardado
        self.logger.finaliza_consulta(query_id, mensaje='Finaliza guardado del csv')
        self.logger._print_line()

        l_cols = df.shape[1]
        l_filas = df.shape[0]
        cons = self.logger.formatter(consultando)
        desc = self.logger.formatter(descargando)
        conv = self.logger.formatter(convirtiendo)
        guar = self.logger.formatter(guardando)
        self.logger._info(
            f'{l_filas:,} filas, '
            f'{l_cols:,} columnas, '
            f'{cons} consultando, '
            f'{desc} descargando, '
            f'{conv} convirtiendo, '
            f'{guar} guardando.'
        )
        self.logger._info(f'Tabla guardada en "{path.absolute()}"')

    def obtener_ultima_ingestion(self, tabla, now=None, primer_dia_mes=False):
        """
        Método público para obtener la última ingestión de una tabla.

        Parameters
        ----------
        tabla : str
            Nombre de la tabla objetivo

        now: dict
            Opcional.
            Fecha a la cual se desea revisar las ingestiones de una tabla
            debe tener las siguientes llaves
            year, month, day

        primer_dia_mes : bool
            True si se desea obtener la primera ingestion del ultimo mes
            de lo contrario debolverá la ultima ingestion disponible a la fecha

        Returns
        -------
        ingestiones: dict
            diccionario con las llaves: year, month, day
        """
        inicio = datetime.now()
        self.logger.info(f'Buscando fechas para {tabla}')
        cn = self.__get_conn()

        def intentar(query, cn, tries=1):
            try:
                return pd.read_sql(query, cn)
            except:
                if tries >= self.max_tries:
                    self.logger.exception(f'Error en la busqueda de ultima ingestion para la tabla {tabla}')
                    raise
                self.logger.warning(
                    f'Error en la busqueda de ultima ingestion para la tabla {tabla}, intentando nuevamente')
                tr = tries + 1
                return intentar(query, cn, tr)

        # Busqueda de particiones -----------------------------------------------------------------
        ingestiones = {}
        df = intentar(f'show table stats {tabla}', cn)
        columns = df.columns

        possible_part = ['year', 'month', 'day']
        parts = {x: y for x in possible_part for y in columns if re.search(x, y)}
        ing = {x: f'ingestion_{x}' for x in possible_part if x not in parts}

        cols = {**parts, **ing}

        if now is not None:
            fecha = now['year'] * 10000 + now['month'] * 100 + now['day']
            filt_p3 = f'where cast({cols["year"]} as int)*10000+cast({cols["month"]} as int)*100+cast({cols["day"]} as int) <= {fecha}'
        else:
            filt_p3 = ''

        # Para busqueda en segunda particion, usado si no encuentra informacion en la primera
        # Particion
        seg_part = False

        if len(parts) > 0:
            df = df[list(parts.values())].apply(pd.to_numeric, errors='coerce').dropna()
            df = df.rename(columns={y: x for x, y in parts.items()})

            df['month'] = df['month'] if 'month' in parts else 0
            df['day'] = df['day'] if 'day' in parts else 0

            df['fecha'] = (df['year'] * 10000) + (df['month'] * 100) + df['day']

            df = df.apply(pd.to_numeric, downcast='integer')

            asc = [False, False, primer_dia_mes]

            if now is not None:
                df = df.query(f'fecha <= {fecha}')

            assert not df.empty, (
                    f"No se encontraron particiones disponibles para la tabla {tabla}" +
                    (f", para la fecha {fecha}" if now is not None else "")
            )

            df = df[possible_part].sort_values(possible_part, ascending=asc)
            last = df.iloc[0].to_dict()
            ingestiones.update(last)

            # Si es necesario buscar en las ingestiones, hacerlo en las ultimas 2 particiones
            # por si no se encuentra una fecha que cumpla las condiciones en la ultima particion
            filt_p1 = '(' + ' and '.join([f'cast({parts[i]} as int)={last[i]}' for i in parts]) + ')'
            if df.shape[0] > 1:
                sec_last = df.iloc[1].to_dict()
                filt_p2 = '(' + ' and '.join([f'cast({parts[i]} as int)={sec_last[i]}' for i in parts]) + ')'
                seg_part = True

        if len(parts) == 3:
            duracion = datetime.now() - inicio
            self.logger.info(
                f'Finalizo la busqueda, duracion: {self.logger.formatter(duracion)}, resultado: {ingestiones}')
            return ingestiones
        # -----------------------------------------------------------------------------------------

        # Busqueda en flujos oozie ----------------------------------------------------------------
        filtro_now = f'and anio_inicio*10000+mes_inicio*100+dia_inicio <= {fecha}' if now is not None else ''

        oozie = intentar(
            f'''
            select
                anio_inicio as year,
                mes_inicio  as month,
                dia_inicio  as day
            from resultados.reporte_flujos_oozie
            where
                lower(nombre_flujo)='{tabla.strip().replace('.', '_').lower()}'
                {filtro_now}
            order by 1 desc, 2 desc, 3 {'asc' if primer_dia_mes else 'desc'} limit 1
            ''',
            cn
        )

        if not oozie.empty:
            ingestiones = oozie.iloc[0].to_dict()
            duracion = datetime.now() - inicio
            self.logger.info(
                f'Finalizo la busqueda, duracion: {self.logger.formatter(duracion)}, resultado: {ingestiones}')
            return ingestiones

        # -----------------------------------------------------------------------------------------

        # Busqueda de ingestiones -----------------------------------------------------------------
        def get_ingest(filtro_p1, filtro_p2):
            if filtro_p1:
                filtro = f"where {filtro_p1} {filtro_p2.replace('where', 'and')}"
            else:
                filtro = filtro_p2
            ing_query = f'''
                select {', '.join(['{} as {}'.format(y, x) for x, y in cols.items()])}
                from {tabla}
                {filtro}
                order by 1 desc, 2 desc, 3 {'asc' if primer_dia_mes else 'desc'}
                limit 1
            '''
            return intentar(ing_query, cn)

        # Intentar primero en la primera particion, si no encuentra informacion, intentarlo
        # en la segunda
        ingest = get_ingest(filt_p1 if 'filt_p1' in dir() else None, filt_p3)
        if ingest.empty and seg_part:
            ingest = get_ingest(filt_p2 if 'filt_p2' in dir() else None, filt_p3)

        fecha = now['year'] * 10000 + now['month'] * 100 + now['day'] if now is not None else None
        assert not ingest.empty, f"No se encontraron ni particiones ni ingestiones disponibles" + (
            f" para la fecha {fecha}" if fecha else "")

        ingestiones.update(ingest[possible_part].iloc[0].to_dict())
        # -----------------------------------------------------------------------------------------
        duracion = datetime.now() - inicio
        self.logger.info(f'Finalizo la busqueda, duracion: {self.logger.formatter(duracion)}, resultado: {ingestiones}')
        return ingestiones

    def _obtener_dataframe(self, query_id, consulta, end='\n',diagnos=False):
        """
        Método privado para la obtención de un DataFrame de pandas para una consulta
        realizada

        Parameters
        ----------
        query_id : int
            id del query dentro del logger de Impala Helper

        consulta : str
            Consulta a ejecutar

        diagnos : bool
            Booleano que activa el proceso de diagnostico de consultas

        Returns
        ------
        dfRet : pandas.DataFrame
            DataFrame de pandas con el resultado de la consulta

        consultando : timedelta
            Tiempo que se tarda la consulta en generar resultados

        descargando : timedelta
            Tiempo que se tarda python en descargar los resultados de la consulta

        convirtiendo : timedelta
            Tiempo que se tarda python en poder convertir los resultados a un DataFrame de pandas
        """
        header, res, consultando, descargando = self._obtener_filas(query_id, consulta, end='', diagnos=diagnos)
        cols = header
        inicio_conversion = datetime.now()
        self.logger._reportar(query_id, estado='convirtiendo')
        dfRet = pd.DataFrame.from_records(res, columns=cols)
        convirtiendo = datetime.now() - inicio_conversion
        # self.logger.finaliza_consulta(query_id, end=end, mensaje='Finaliza obtener dataframe')
        return dfRet, consultando, descargando, convirtiendo

    def obtener_dataframe(self, consulta, params=None, diagnostico=False):
        """
        Método público para ejecutar una query y guardar los resultados en un
        data frame de pandas

        Parameters
        ----------
        consulta : str
            Consulta parametrizada
        params : str
            Diccionario de parámetros de consulta
        diagnostico : bool
            Booleano que activa el proceso de diagnostico de consultas

        Returns
        ------
        dfRet : pandas.DataFrame
        """
        consulta = self._eliminar_comentarios(consulta)
        consulta = consulta.format(**params) if params else consulta
        self.__set_params_control(params, type="ctrl")
        self.__set_params_control(params, type="tbl")
        index = self.logger.establecer_queries('', [consulta])
        query_id = index[0]
        self.logger._modificar_tipo(query_id, 'DATAFRAME')

        self.logger._print_encabezado()
        self.logger.inicia_consulta(query_id)
        dfRet, consultando, descargando, convirtiendo = self._obtener_dataframe(query_id, consulta, diagnos=diagnostico)
        self.logger.finaliza_consulta(query_id, mensaje='Finaliza obtener_dataframe')
        self.logger._print_line()

        l_cols = dfRet.shape[1]
        l_filas = dfRet.shape[0]
        cons = self.logger.formatter(consultando)
        desc = self.logger.formatter(descargando)
        conv = self.logger.formatter(convirtiendo)
        self.logger._info(
            f'{l_filas:,} filas, {l_cols:,} columnas, {cons} consultando, {desc} descargando, {conv} convirtiendo')

        return dfRet

    def obtener_dataframe_archivo(self, ruta, params=None, id_consulta=0):
        """
        Método público para obtener un DataFrame de Pandas 
        desde una consulta de un archivo

        Parameters
        ----------

        ruta : str
            Ruta local del archivo

        params : dict, Opcional
            Diccionario de parámetros

        id_consulta : int, Opcional, default=0
            Identificador de la consulta. Las diferentes consultas del archivo serán indizadas iniciando en 0


        Returns
        -------

        pandas.DataFrame
            DataFrame de pandas con el resultado de la ejecución de la consulta indicada
        """
        consultas = self._obtener_consultas(ruta, params)[1]
        return self.obtener_dataframe(consultas[id_consulta])

    def imprimir_plan(self):
        """
            Metodo publico para poder imprimir todo el plan de ejecucion seguido por el
            modulo de Impala Helper
        """
        self.logger.imprimir_plan()

    def imprimir_resumen(self, groupby=['documento', 'estado']):
        """
            Metodo publico util para mostrar de manera resumida el plan y el estado
            de ejecucion realizado por Impala Helper

            Parameters
            ----------
            groupby : str or list[str]
                Llaves por las cuales se va a agrupar y mostrar el resumen. Las llaves
                permitidas son: [`documento`,`estado`,`tipo`,`nombre`]
        """
        self.logger.imprimir_resumen(groupby)

    def _obtener_iterador(self, query_id, consulta, end='\n', diagnos=False):
        """
        Método privado para obtener un iterador a los resultados de una consulta

        Parameters
        ----------
        consulta : str
            Consulta a ejecutar
        params : dict
            Diccionario de parámetros de consulta
        diagnos : bool
            Booleano que activa el proceso de disgnostico de consulta

        Returns
        ------
        cursor : pyodbc.Cursor
            Cursor de ejecución

        resultados : it
            Iterador sobre resultados de consulta
        """
        cursor = self.__get_conn().cursor()
        try:
            return self._ejecutar_consulta(cursor, query_id, consulta, end, diagnos=diagnos)
        except Exception as e:
            self.logger.error_query(query_id, e, False)
            raise

    def _reStarter(self, cursor, query_id, consulta, con_resumen=False, end='\n', tries=1):
        """
        Método privado para reiniciar ejecución de consulta

        Parameter
        ----------
        cursor : str
            Cursor para ejecutar consulta
        consulta : str
            Consulta a ejecutar
        params : dict
            Diccionario de parámetros de consulta
        """
        # Generar reporte en el logger
        # self.logger.inicia_consulta(query_id)
        if tries != 1:
            self.logger._reportar(query_id, estado=f'intento {tries}')
        try:
            # Intentar
            cursor.execute(consulta)
            # self.logger.finaliza_consulta(query_id, end=end)
            return cursor
        except pyodbc.ProgrammingError as e:
            self.logger._reportar(query_id, estado='error')
            self.logger.log.exception("Error de programacion")
            raise
        except pyodbc.Error as e:
            # recoger el error y verificar su contenido
            txt = e.args[1] if len(e.args) >= 2 else str(e)
            # Verificar si es un error de analisis
            if re.search(
                    'parseexception|analysisexception|authorizationexception|ssl_connect|ssl_ctx_load_verify_locations',
                    txt, re.IGNORECASE):
                # De serlo levantar el error
                self.logger._reportar(query_id, estado='error')
                self.logger.log.exception('Error en ejecucion de consulta')
                raise
            # De lo contrario verificar el numero de intentos establecido
            if tries >= self.max_tries:
                # Si ya los agotó levantar el error
                self.logger._reportar(query_id, estado='error')
                self.logger.log.exception('Error en ejecucion de consulta')
                raise
            # De lo contrario traer el registro del logger para verificar si
            # es una consulta de tipo "CREATE"
            registro = self.logger.df.loc[query_id]
            if self.logger._get_type(consulta) == 'CREATE':  # if registro['tipo'] == 'CREATE':
                if re.match(r'create\s+(external\s+)?table\s+if\s+not\s+exists', consulta, re.IGNORECASE):
                    # Si es un "CREATE TABLE IF NOT EXISTS" se debe 
                    # ejecutar de nuevo la consulta
                    self.logger.log.info(f'Falló la consulta, se procede a intentar nuevamente...')
                elif re.match(r'create\s+table\s+resultados', consulta, re.IGNORECASE):
                    # Si es un "CREATE TABLE" de una tabla en ZDR, se debe
                    # ejecutar de nuevo la consulta
                    self.logger.log.info(f'Falló la consulta, se procede a intentar nuevamente...')
                elif re.match(r'create\s+(external\s+)?table', consulta, re.IGNORECASE):
                    # Si es un "CREATE TABLE", traerme la tabla que se está intentando crear
                    # para borrarla e intentar nuevamente
                    tabla = registro['nombre']
                    if tabla:
                        consulta_borrado = f'drop table if exists {tabla} purge'
                        self.logger.log.info(
                            f'Falló la consulta, se procede a hacer DROP de la tabla para crearla nuevamente')
                        self._reStarter(cursor, query_id, consulta_borrado, con_resumen, end, tries=1)
                    else:
                        self.logger._reportar(query_id, estado='error')
                        self.logger.log.exception('Error en ejecucion de consulta')
                        raise
                elif re.match(r'create view', consulta, re.IGNORECASE):
                    tabla = registro['nombre']
                    if tabla:
                        cursor.execute(f'drop view if exists {tabla}')
                    else:
                        self.logger._reportar(query_id, estado='error')
                        self.logger.log.exception('Error en ejecucion de consulta')
                        raise
            elif self.logger._get_type(consulta) == 'INSERT':
                if re.search(r'insert\s+into', consulta, re.IGNORECASE):
                    self.logger._reportar(query_id, estado='error')
                    self.logger.log.exception('Error en ejecucion de consulta')
                    raise
            # Reportar el fallo
            self.logger._reportar(query_id, estado=f'falló n.{tries}', end='\n')
            txt1 = ('\n' + txt).replace('\n', '\n            ')
            self.logger.log.error(txt1)
            # Intentarlo nuevamente
            tri = tries + 1

            if self.sleep_reintento > 0:
                self.logger.log.info(
                    'Se procede a dormir durante {0} segundos antes de reintentar'.format(self.sleep_reintento))
                time.sleep(self.sleep_reintento)

            return self._reStarter(cursor, query_id, consulta, con_resumen, end, tries=tri)
        except Exception as e:
            self.logger._reportar(query_id, estado='error')
            self.logger.log.exception('Error en ejecucion de consulta: {0}'.format(e))
            raise e

    def _obtener_conexion(self, dsn=None, username=None, password=None):
        """
        Método privado para obtener una conexión

        Returns
        ------
        conn : pyodbc.connection
            Conexión por pyodbc
        """
        try:
            if not dsn is None:
                if type(dsn) != str:
                    raise ValueError("EL valor de dsn debe ser un string")
                    
                elif os.environ.get("MAQUINA") == "CDP-PC":
                    user = os.environ.get("OOZIE_CDP_USER")
                    pwd = os.environ.get("DECRYPT_USER")
                    
                    dsn = f"{dsn};uid={user};pwd={pwd}"

                conn = pyodbc.connect(f"DSN={dsn}", autocommit=True)
                return conn

        except Exception as e:
            raise Exception("Error en la conexión con DSN, error: {0}".format(e))

        try:
            conn = pyodbc.connect(
                self._obtener_str_conexion(
                    platform.system(),
                    "1" if username is None else "3",
                    username,
                    password
                ),
                autocommit=True
            )
            return conn
        except Exception as e:
            if username is None:
                self.logger.warning('Error conectando con Kerberos, intentando con Username y Password')
                conn = pyodbc.connect(
                    self._obtener_str_conexion(platform.system(), "3", username, password),
                    autocommit=True
                )
                return conn
            else:
                raise

    def _obtener_str_conexion(self, system, auth_mech, username=None, password=None):
        """
        Método privado para obtener una conexión

        Parameters
        ----------
        system : str
            Sistema operativo en el que está instalado el paquete
        """
        if system == 'Linux':
            str_conexion = "DSN=impala-virtual-prd"
        else:
            if auth_mech == "1":
                str_conexion = f'''
                    Host=impala.bancolombia.corp;
                    Port=21050;
                    AuthMech=1;
                    SSL=1;
                    KrbRealm=BANCOLOMBIA.CORP;
                    KrbFQDN=impala.bancolombia.corp;
                    KrbServiceName=impala;
                    TrustedCerts={self._obtener_ruta_cert()};
                '''

            elif auth_mech == "3":
                user = username if username is not None else input("Usuario: ")
                pwd = password if password is not None else getpass.getpass(f"{user} password: ")
                str_conexion = f'''
                    Host=impala.bancolombia.corp;
                    Port=21050;
                    AuthMech=3;
                    UID={user}@bancolombia.corp;
                    PWD={pwd};
                    SSL=1;
                    KrbRealm=BANCOLOMBIA.CORP;
                    KrbFQDN=impala.bancolombia.corp;
                    KrbServiceName=impala;
                    TrustedCerts={self._obtener_ruta_cert()};
                '''

            if system == 'Darwin':
                str_conexion = 'Driver=/opt/cloudera/impalaodbc/lib/universal/libclouderaimpalaodbc.dylib;' + str_conexion
            elif system == 'Windows':
                str_conexion = 'Driver=Cloudera ODBC Driver for Impala;' + str_conexion

        return str_conexion

    def _obtener_consultas(self, ruta, params):
        """
        Método privado para obtener consultas de Impala de un archivo

        Parameters
        ----------
        ruta : str
            Ruta local del archivo
        params : dict
            Diccionario de parámetros

        Returns
        -------
        lista_consultas : list
            Lista de consultas
        """
        lista_consultas = []

        texto = ''
        with open(ruta, encoding='UTF-8') as infile:
            texto = self._eliminar_comentarios(infile.read())

        texto = texto.format(**params) if params else texto
        lista_consultas = [x.strip() for x in sqlparse.split(texto) if len(x.strip()) > 1]

        nombre = ruta.replace('\\', '/').split('/')[-1]
        index = self.logger.establecer_queries(nombre, lista_consultas)

        return index, lista_consultas

    def _diagnostico_consulta(self, cursor, consulta, query_id):
        """
        Método privado para diagnosticar el consumo de recursos de las consultas

        Parameters
        ----------
        cursor : pyodbc.cursor
            cursor encargado de ejecutar la consulta 
        consulta : str
            Consulta parametrizada
        query_id : int
            id del query dentro del logger de Impala Helper

        Returns
        ------

        """
        self.logger._reportar(query_id, estado='diagnosticando')
        diagnos_consulta = 'EXPLAIN ' + consulta # Inyección de explain en cada consulta
        diagnos_cursor = self._reStarter(cursor, query_id, diagnos_consulta)
        diagnos_res = []
        i = 0;
        try:
            while True:
                records = diagnos_cursor.fetchmany(self.fetch_size)
                i = i + len(records)
                if len(records) == 0:
                    break;
                diagnos_res.extend(records)
        except Exception as e:
            self.logger.exception(e)
            raise
        
        diagnos_res = [res[0] for res in diagnos_res]
        diagnos_res = " ".join(diagnos_res)
        pass_memory = True
        pass_hdfs_scan = True
        pass_stats = True
        
        # Check de consumo de memoria
        memory = re.findall("Per-Host Resource Estimates: Memory=(.*?(?:KB|MB|GB|TB|PT)+)",diagnos_res)
        if len(memory) > 0 :
            memory_type = memory[0][-2:]
            memory_size = float(memory[0][:-2])

            if((memory_type == 'GB' and memory_size > 20) or memory_type in ['TB','PT']):
                pass_memory = False
        
        # Check HDFS Partitions
        hdfs_part = re.findall("HDFS partitions=.*?size=(.*?(?:KB|MB|GB|TB|PT)+)",diagnos_res)
        for m in hdfs_part:
            memory_type = m[-2:]
            memory_size = float(m[:-2])
            if((memory_type == 'TB' and memory_size > 10) or memory_type in ['PT']):
                pass_hdfs_scan = False

        # Check comute stats  
        missing_stats = re.findall("The following tables are missing relevant table and/or column statistics.(.*?)PLAN-ROOT SINK",diagnos_res)
        msg_stats = "WARNING: Se esta consultando una tabla con estadísticas desactualizadas"
        if len(missing_stats) > 0:
            if " resultados" in missing_stats[0] or " proceso" in missing_stats[0]:
                pass_stats = False
        
        msg_memory = "Query tiene un alto consumo de memoria : {}. Permitido <= 20GB".format(memory)
        self.logger.diagnostico_consulta("Memoria Estimada: {}".format(str(memory[0])))
        msg_hdfs = "Query tiene un alto consumo de recursos en lectura de archivos HDFS : {}{}. Permitido <= 10GB".format(hdfs_part[0][:-2], hdfs_part[0][-2:])
        self.logger.diagnostico_consulta("Particiones HDFS: {}".format(str(hdfs_part[0])))
        
        msgs = [msg_memory,msg_hdfs,msg_stats]
        pass_ = [pass_memory,pass_hdfs_scan,pass_stats]

        for i in range(0,len(pass_)):
            if not pass_[i] and i < 2:
                raise Exception(msgs[i])
            elif not pass_[i]:
                self.logger.print('')
                self.logger.warning(msgs[i])

    def _ejecutar_consulta(self, cursor, query_id, consulta, end='\n', diagnos = False):
        """
        Método privado para obtener un cursor

        Parameters
        ----------
        cursor : pyodbc.cursor
            cursor encargado de ejecutar la consulta

        query_id : int
            id del query dentro del logger de Impala Helper

        consulta : str
            Consulta a ejecutar

        diagnos : bool
            Booleano que activa el proceso de diagnostico de consultas  

        Returns
        -------
        cursor : pyodbc.cursor
            cursor iterable con los resultados de la consulta
        """
        # Obtener el tipo de consulta
        tipo = self.logger._get_type(consulta)

        # Aplica para ejecución de compilación con porción parcial de datos
        tablesample_types = ['CREATE', 'SELECT', 'WITH']
        if self.porcentaje_limit > 0 and self.porcentaje_limit < 100 and tipo in tablesample_types:  
            consulta = self.__inyectar_tablesample(consulta)

        # Ejecutar diagnostico de consulta
        diagnos_types = ['CREATE', 'DELETE', 'INSERT', 'SELECT', 'UPDATE', 'UPSERT', 'VALUES', 'WITH']
        if diagnos and tipo in diagnos_types:
            self._diagnostico_consulta(cursor, consulta, query_id)

        # Ejecución de consulta
        res_consulta = self._reStarter(cursor, query_id, consulta, end=end)

        # Controles en tiempo real
        if self.ctrls != {}:
            if tipo in ['CREATE', 'INSERT']:
                tabla = self.logger._get_table(consulta, tipo)
                self.__check_control(self.ctrls, tabla)

        return res_consulta

    def _eliminar_comentarios(self, text):
        """
        Método privado para eliminar todos los comentarios tanto
        los multiliena (/* comentatio */) como de una sola linea (-- comentario),
        Además de remover las lineas vacias o llenas unicamente de espacios.

        Parameters
        ----------
        text : str
            Texto con comentarios

        Returns
        -------
        text : str
            Texto sin comentarios
        """
        # BY: Onur Yıldırım -> https://stackoverflow.com/questions/2319019/using-regex-to-remove-comments-from-source-files
        # MODIFIED BY: Walter Mauricio Salazar Morales

        # first group captures quoted strings (double or single)
        quoted1 = r'"(?:.*?(?:\\")?)*?"'  # Double quoted -> "jljkjl"
        quoted2 = r"'(?:.*?(?:\\')?)*?'"  # Single quoted -> 'jdlakj'

        # second group captures comments and blank lines(--single-line or /* multi-line */)
        multi = r'/\*.*?\*/[\n\r]*'  # Multiline comment                        -> /* comment */
        f_sing = r'(?:^\s*--[^\n\r]*[\n\r]+)+'  # Group of single comments of entire lines -> -- comment
        single = r'--.*?$'  # Single comments                          -> content -- comment
        blank = r'^\s*[\n\r]+'  # Blank lines (empty or just spacing)      -> \n

        pattern = f'({quoted1}|{quoted2})|({multi}|{f_sing}|{single}|{blank})'

        regex = re.compile(pattern, re.MULTILINE | re.DOTALL)

        def _replacer(match):
            # if the 2nd group (capturing comments) is not None,
            # it means we have captured a non-quoted (real) comment string.
            if match.group(2) is not None:
                return ""  # so we will return empty to remove the comment
            else:  # otherwise, we will return the 1st group
                return match.group(1)  # captured quoted-string

        return regex.sub(_replacer, text)

    def _obtener_ruta_cert(self):
        return pkg_resources.resource_filename(__name__, 'data/cert/cachain.pem')

    def __process_alias_regex(self,consulta, orig_regex_alias):
        orig_alias = re.findall(orig_regex_alias, consulta)
        orig_alias_list = []

        for match in orig_alias:
            orig_alias_list.append(''.join(list(match)))
        return orig_alias_list

    def _process_match_orig(self, orig_regex, orig_regex_alias, consulta):

        reserved = ('on', 'where', 'left', 'join', 'union', 'inner', 'order', 'group', 'limit')

        # Tabla origen sin alias
        orig_direct = re.findall(orig_regex, consulta)

        # Tabla origen con Alias
        orig_alias = self.__process_alias_regex(consulta, orig_regex_alias)

        orig = orig_direct

        if len(orig_direct) == len(orig_alias):
            orig_check = [orig_direct[i] in orig_alias[i] for i in range(0, len(orig_direct))]
            assert False not in orig_check, 'Error en identificación de tablas origen'

            for i in range(0, len(orig_alias)):
                if orig_alias[i].lower().endswith(reserved):
                    orig_alias[i] = orig_direct[i]

            orig = orig_alias

        return orig

    def __obtener_tablas_origen_destino(self, consulta):
        """
        Método privado para identificar las tablas
        origen - destino desde una consulta

        Parameters
        ----------
        consulta : str
            Query para realizar la extracción de tablas
            origen - destino

        Returns
        -------
        tables : list<str>
            Lista con duplas de fuentes en estructura
            origen - destino
        """
        # Metodo de identificación reutilizado de
        # https://dev.azure.com/GrupoBancolombia/Vicepresidencia%20de%20Innovaci%C3%B3n%20y%20Transformaci%C3%B3n%20Digital/_git/vrgo-gretel
        try:
            tables = []
            # Expresiones regulares para identificar tablas
            dest_regex = re.search(
                '(?:insert\s+into\s+table\s+|insert\s+into\s+|insert\s+overwrite\s+|create\s+table\s+if\s+not\s+exists\s+|create\s+table\s+|create\s+external\s+table\s+|select\s+)([^\s]+)',
                consulta, re.IGNORECASE)
            orig_regex = re.compile('(?:from\s+|\s+join\s+)([^\s\()]+)', re.IGNORECASE)
            orig_regex_alias = re.compile('(?:from\s+|\s+join\s+)([^\s\()]+)(\s)*(as)*(\s)+([^\s\()]+)*', re.IGNORECASE)
            with_regex = re.compile('(?:with\s+|,\s*)(\w+)\s+as', re.IGNORECASE)

            orig = self._process_match_orig(orig_regex, orig_regex_alias, consulta)

            # Asignacion zonas de proceso para la consulta                            
            self.tables_proc = [tbl for tbl in orig_regex.findall(consulta) if re.match('proceso(s){0,1}', tbl)] if self.list_tbl_porc else []

            with_list = []
            for subtabla in re.findall(with_regex, consulta):
                with_list.append(subtabla.lower())

            if consulta.lstrip('\n\r').lower().startswith(('create', 'insert')):
            #if "create" in consulta.lower() or "insert" in consulta.lower():
                for (tabla) in orig:
                    if tabla.lower().rstrip("\n\r") not in with_list:
                        orig = tabla
                        dest = dest_regex.group(1)
                        if orig != dest:
                            tables.append((orig.lower().rstrip("\n\r"),
                                           (re.sub('[^A-Za-z0-9_.]+', '', dest).lower().rstrip("\n\r"))))
            elif consulta.lstrip('\n\r').lower().startswith(('select','with')):
                for (tabla) in orig:
                    if tabla.lower().rstrip("\n\r") not in with_list:
                        orig = tabla
                        tables.append((orig.lower().rstrip("\n\r"), ''))
            # if len(tables) == 0:
            #     self.logger.error('No se encontraron tablas para la compilación\n')
            return tables

        except Exception as err:
            self.logger.error("Falló la identificación de tablas, posible causa del error: \n{0}".format(err))

    def __obtener_tablas_src(self, consulta):
        """
        Método privado para identificar las tablas origen
        pertenecientes a zona de resultados o zona de
        datos crudos desde una consulta

        Parameters
        ----------
        consulta : str
            Query para realizar la extracción de tablas
            origen de ZDC o ZDR

        Returns
        -------
        tables : list<str>
            Lista de tablas origen de la consulta que
            se identificaron como parte de ZDC o ZDR
        """
        # Metodo de identificación reutilizado de
        # https://dev.azure.com/GrupoBancolombia/Vicepresidencia%20de%20Innovaci%C3%B3n%20y%20Transformaci%C3%B3n%20Digital/_git/vrgo-gretel
        try:
            tables = self.__obtener_tablas_origen_destino(consulta)
            if len(tables) > 0:
                src_tables = list(set([no[0].strip() for no in tables if no[0] not in [nd[1] for nd in tables] \
                                       and re.match(
                                                    '^s_[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]|^resultado(s){0,1}_[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]',
                                                    no[0].strip().lower())]))
                src_tables.sort(key=len, reverse=True)
                # if len(src_tables) == 0:
                #     self.logger.warning('No se encontraron tablas crudas o de resultados para la compilación\n')
                return src_tables
        except Exception as err:
            self.logger.error(
                "Falló la identificación de tablas crudas o de resultados, posible causa del error: \n{0}".format(err))

    def __inyectar_tablesample(self, consulta):
        """
        Método privado para inyectar una sentencia
        de tipo TABLESAMPLE SYSTEM(n) para las consultas con fuentes
        de zona de datos crudos o zona de resultados.
        Aplica para ejecuciones de compilación

        Parameters
        ----------
        consulta : str
            Query para realizar la extracción de tablas
            fuente y en el cuál se aplica la sentencia TABLESAMPLE SYSTEM(n)

        Returns
        -------
        consulta : int
            Query con el TABLESAMPLE SYSTEM(n) inyectado cuando se cumplan
            las condiciones. Los registros se limitan proporcionalmente
            utilizando el argumento porcentaje_limit
        """
        # if "limit" not in (re.sub("\d*\s*", "", consulta).lower()[-6:]):
        target_tables = self.__obtener_tablas_src(consulta)

        if target_tables is not None and len(target_tables) > 0:
            target_tables = [table.replace("\n","").replace(";","").strip() for table in target_tables]
            if self.list_tbl_porc and '*' not in self.list_tbl_porc:
                target_tables = [table for table in target_tables if table.split(' ')[0] in self.list_tbl_porc]
            target_encod = [(re.sub('\s+', ' ', target_tables[i]), f'ts@@@{i}') for i in range(0, len(target_tables))]
            cons = consulta.lower().rstrip("\n\r")
            cons = re.sub('\s+', ' ', cons)
            for i in range(0, len(target_encod)):
                cons = cons.replace(target_encod[i][0], target_encod[i][1])
            for i in range(0, len(target_encod)):
                cons = cons.replace(target_encod[i][1], f"{target_encod[i][0]} TABLESAMPLE SYSTEM({self.porcentaje_limit})")
                self.logger.print('')
                self.logger.info(f"TABLESAMPLE INYECTADO {target_encod[i][0]} {self.porcentaje_limit}%")
            consulta = cons
        elif self.tables_proc is not None and len(self.tables_proc) > 0:
            tbl_proc = " \n[*] ".join(list(set([tbl for tbl in self.tables_proc])))
            self.logger.print('')
            self.logger.warning(f"No se puede aplicar el porcentaje ya que es una tabla de zona de proceso: {tbl_proc}".format(tbl_proc=tbl_proc))

        return consulta

    def __check_params_control(self, controls):
        """
        Método privado para evaluar los parámetros de los controles

        Parameters
        ----------
        controls: dict
            Diccionario con los controles a evaluar

        Returns
        -------

        """
        operators = ["==", ">", "<", "<=", ">="]
        for control in controls:
            ctrl = controls.get(control)

            assert isinstance(ctrl, dict), \
                f"Los controles por tabla se configuran mediante diccionarios: {control} {ctrl}"

            assert ctrl.get('control') is not None, \
                f"Debe existir por lo menos la llave control: {control} {ctrl}"

            assert isinstance(ctrl.get('control'), list), \
                f"Control debe ser una lista: {control} {ctrl}"

            assert ctrl is not None and len(ctrl.get('control')), \
                f"Control no debe ser None o vacío: {control} {ctrl}"

            assert len(ctrl.get('control')) in [2, 4], \
                f"Lista de control mal configurada: {control} {ctrl}"

            if len(ctrl.get('control')) == 2:
                assert isinstance(ctrl.get('control')[0], str) \
                       and ctrl.get('control')[0].strip() in operators, \
                    f"Operador incorrecto (En la primera posición debe indicar el operador lógico): {control} {ctrl}"

                assert isinstance(ctrl.get('control')[1], int), \
                    f"Operador incorrecto (Debe indicar un valor numérico para el control): {control} {ctrl}"

            if len(ctrl.get('control')) == 4:
                assert isinstance(ctrl.get('control')[0], str) \
                       and isinstance(ctrl.get('control')[2], str) \
                       and ctrl.get('control')[0].strip() in operators \
                       and ctrl.get('control')[2].strip() in operators, \
                    f"Operador incorrecto (En la primera y tercera posición debe indicar el operador lógico): {control} {ctrl}"

                assert isinstance(ctrl.get('control')[1], int) \
                       and isinstance(ctrl.get('control')[3], int), \
                    f"Operador incorrecto (Debe indicar valores numéricos para el control): {control} {ctrl}"

                assert ctrl.get('control')[1] <= ctrl.get('control')[3], \
                    f"Debe indicar primero el limite inferior y luego el superior: {control} {ctrl}"

    def __check_control(self, controls, tabla):
        """
        Método privado para evaluar los controles de compilación ingresados por el usuario

        Parameters
        ----------
        controls: dict
            Diccionario con la configuración de controles
        tabla: str
            Tabla sobre la cual evaluar el control

        Returns
        -------

        """
        query = """
                    SELECT COUNT(*) AS nro_rtros
                    FROM {TABLE}
                    {FILTER}
                 """
        control = controls.get(tabla)
        if control is not None:
            ctrl = control.get('control')
            query_ctrl = query.format(TABLE=tabla, FILTER=control.get('filter', ''))
            value = self.obtener_dataframe(query_ctrl).nro_rtros[0]
            operation = ''

            if len(ctrl) == 4:
                operation = "{num} {op1} {lim1} and {num} {op2} {lim2}".format(num=value,
                                                                               op1=ctrl[0],
                                                                               lim1=ctrl[1],
                                                                               op2=ctrl[2],
                                                                               lim2=ctrl[3])
            elif len(ctrl) == 2:
                operation = "{num} {op1} {lim1}".format(num=value,
                                                        op1=ctrl[0],
                                                        lim1=ctrl[1])
            if eval(operation):
                self.logger.print('')
                self.logger.info(f"CONTROL COMPILACIÓN EXITOSO: {tabla} {ctrl} NRO_REGS =  {value}")
            else:
                raise Exception(f"CONTROL COMPILACIÓN NO SUPERADO: {tabla} {ctrl} FAIL NRO_REGS =  {value}")
