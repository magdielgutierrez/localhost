import re
import pandas as pd
import pkg_resources
import json
import os
from .utils import Utils
from .logger import Logger
from datetime import datetime
from dateutil import parser
from dateutil.relativedelta import relativedelta
from scipy import stats

class Analisis():
    
    """
    Clase del maestro de validaciones. Contiene todas las funciones necesarias para implementar
    Analisis (periodicidad y variacion) dentro de los controles del maestro de validaciones.
    """

    def __init__(self,  
                sparky= object, 
                logger = object,
                params: dict = None,
                **kwargs):
        """
        Args:
            self
            sparky (object, optional): Instancia de sparky. Defaults to object
            logger (object, optional): Instancia del logger de sparky. Defaults to object
        """
        if params is None:
            params = {}
        self.sp = sparky
        self.log = logger
        self.params = params
        self.ruta = Utils._obtener_ruta()
        for key, value in kwargs.items(): 
            setattr(self, key, value)
    

    def gestionar_periodos(self) -> pd.DataFrame:
        '''
        Función que orquesta la obtención de los datos necesarios para la asignación de periodos a cada control
        Args:
            self
        Returns:
            self.df_controles (pandas.DataFrame): DataFrame de controles actualizado con la información de periodos 
        '''
        self.df_controles = self.params['df_controles']
        if 'periodicidad' in self.df_controles.columns:
            self.df_controles['periodicidad'].fillna(0,inplace=True)
            if self._obtener_periodos():
                    self._asignar_periodos()
            return self.df_controles
        else:
            raise self.log.list_error('flujos',0)
        

    def _obtener_periodos(self) -> bool:
        '''
        Función privada que obtiene los periodos de ejecución de cada control en base a las ejecuciones anteriores
        Args:
            self
        Returns:
            bool: True o False si se logró obtener la información de periodos
        """
        '''
        params={}
        self.log.print_periodos()
        df_describe = Utils._describir_tabla(zona_tabla_output=self.params['zona_tabla_output'], sparky=self.sp, logger=self.log)

        if df_describe.empty:

            self.df_controles['periodo'] = -1
            self.log.print_info('''la tabla output aun no esta creada en la LZ y no se puede consultar si hubo
                                ejecuciones anteriores con el maestro. Esta ejecucion sera tomada como la primera
                                para determinar los periodos''')
            return False     
        else:
            self.params["columns"] = Utils.eliminar_columnas(df_describe)
            self.params["columns_name"] = Utils._columns_namestypes(df_describe)
            

        if 'A.periodo' in self.params["columns_name"]:
            self.key_columns = ['A.nombre_flujo', 'A.etapa_ejecucion', 'A.tabla', 'A.nom_columna', 'A.ctrl', 'A.parametro_ctrl']

            if all(item in self.params["columns_name"] for item in self.key_columns):
                self.params["zona_tabla"] = self.params['zona_tabla_output']
                ruta_select = "{}/sql/select.sql".format(self.ruta)
                with open(ruta_select) as in_file_sql:
                    select_sql = in_file_sql.read()
                self.df_periodo = self.sp.helper.obtener_dataframe(consulta=select_sql,params=self.params)
                return True
            else:
                self.df_controles['periodo'] = -1
                self.log.print_info('''la tabla output no contiene las columnas clave para la ejecucion del maestro con
                                    periodicidad, por tanto no se puede verificar este parametro en las ejecuciones anteriores.
                                    Esta ejecucion sera tomada como la primera para determinar los periodos''')
                return False
        else:
            self.df_controles['periodo'] = -1
            self.log.print_info('''la tabla output no contiene la columna "periodo" por tanto no se puede verificar este
                                parametro en las ejecuciones anteriores. Esta ejecucion sera tomada como la primera para
                                determinar los periodos''')
            return False

    def _asignar_periodos(self) -> bool:
        """
        Función privada que asigna la información sobre el periodo de ejecución actual de los controles
        Args:
            self
        Returns:
            bool: True o False si se logró o no asignar la información
        """
        self.key_columns = ['nombre_flujo', 'etapa_ejecucion', 'tabla', 'nom_columna', 'ctrl', 'parametro_ctrl']
        if all(col in self.df_controles.columns for col in self.key_columns):
            self.df_controles[self.key_columns] = self.df_controles[self.key_columns].astype(object)
            self.df_periodo[self.key_columns] = self.df_periodo[self.key_columns].astype(object)
            key_columns_p = self.key_columns.copy()
            key_columns_p.append('periodo')
            self.df_controles = pd.merge(self.df_controles,self.df_periodo[key_columns_p], on=self.key_columns,how='left')
            if self.df_controles['periodo'].dtype != 'int64':
                self.df_controles['periodo'] = pd.to_numeric(self.df_controles['periodo'],errors='coerce').round(0).fillna(-1).astype('int64')
                self.log.print_warning('''el dtype de la columna periodo no era el correspondiente. La columna ha sido convertida a int64,
                                       los valores numericos redondeados y los no numericos o nulos asignados a -1''')
            return True
        else:
            self.df_controles['periodo'] = -1
            self.log.print_warning('''la tabla input no tiene las columnas claves necesarias para asignar periodos a los controles.
                                   Esta ejecucion sera tomada como la primera para determinar los periodos''')
            return False
    
    def gestionar_periodos_nulos(self, df_filtrado) -> pd.DataFrame:
        """
        Función que gestiona el maestro en caso de que el periodo actual no tenga controles para ejecutar
        Args:
            self
            df_filtrado (pandas.DataFrame): Dataframe con los controles
        Returns:
            df_filtrado (pandas.DataFrame): Dataframe de salida del maestro
        """
        df_filtrado['f_ejecucion'] = datetime.now()

        column_list = self.params["columns"]["name"].to_list()

        for column in column_list:
            if column not in df_filtrado.columns:
                df_filtrado[column] = None
        
        columnas_adicionales = [col for col in df_filtrado.columns if col not in column_list]

        orden_columnas = column_list + columnas_adicionales
        df_filtrado = df_filtrado[orden_columnas]

        return df_filtrado


    def _aumentar_periodo(self, df) -> pd.DataFrame:
        """
        Función que aumenta en 1 el periodo de cada control o lo reinicia en 0 si ya se cumplió la periodicidad
        Args:
            self
            df (pandas.DataFrame): Dataframe con los controles
        Returns:
            df (pandas.DataFrame): Dataframe con los controles con el periodo aumentado
        """
        for index,row in df.iterrows():
            if row['periodo'] == row['periodicidad']:
                df.at[index, 'periodo'] = 0
            else:
                df.at[index, 'periodo'] = row['periodo'] + 1
        return df

    
    def gestionar_variacion(self, ctrl_instance, df: pd.DataFrame = pd.DataFrame()) -> pd.DataFrame:
        '''
        Función que orquesta la obtención de los datos necesarios para el analisis de variación de cada control y
        los asigna en la columna (past_result)
        Args:
            self
            ctrl_instance (object): Instancia de la clase control
            df (pandas.DataFrame): Dataframe con la información de controles a analizar
        Returns:
            df (pandas.DataFrame): Dataframe actualizado con la información pasada de los controles
        
        '''
        self.log.print_variacion()
        df_describe = Utils._describir_tabla(zona_tabla_output=self.params['zona_tabla_output'], sparky=self.sp, logger=self.log)
        
        if df_describe.empty:
            self.log.print_info('''la tabla output aun no esta creada en la LZ y no se puede consultar si hubo
                                ejecuciones anteriores con el maestro. Esta ejecucion sera tomada como la primera
                                para determinar la variacion historica y singular''')

        for index, row in df.iterrows():
            if df.at[index,'variacion'] == 1:
                tipo_variacion = df.at[index,'param_variacion'].split(':')[0] if ':' in df.at[index,'param_variacion'] else ''
                df.at[index,'tipo_variacion'] = tipo_variacion
                if tipo_variacion == 'hist' or tipo_variacion == '':
                    df.at[index, 'past_result'] = self._obtener_dato_historico(row, df_describe)
                elif tipo_variacion in ('ing','part', 'est'):
                    df.at[index, 'past_result'] = self._obtener_dato_tabla(ctrl_instance, row, tipo_variacion)
                else:
                    raise self.log.list_error('flujos',1)
        return df
    
    
    def calcular_variacion(self, row, present_value) -> str:
        '''
        Función que calcula, en base al metodo definido, la variación del valor numerico del control y define
        si pasa o no pasa
        Args:
            self
            row (pandas.Series): Fila del control
            present_value (float): Valor presente del control
        Returns:
            estado_variacion (str): Estado actual de la variación del control, puede ser OK si la validación
                                    paso o indicar caso_falla en caso de que no
        """
        '''

        estado_variacion, salida = "OK", 0

        if row['tipo_variacion'] == 'est':
            variacion = self._calcular_estacionalidad(row)
            if not variacion or variacion > 0.05:
                estado_variacion = row["caso_falla"]
                self.log.print_warning(" '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' ")
                self.log.print_warning("El control {0} a nivel {1} en la tabla {2} no pasó la validacion de estacionalidad.".format(row['ctrl'], row['nivel'], row['tabla']))
            if not variacion:
                self.log.print_warning("Tenía todos los valores iguales en la serie de tiempo")
                self.log.print_warning(" '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' ")
            if variacion > 0.05:
                self.log.print_warning("El valor p del Test de Kruskal fue >0.05")
                self.log.print_warning(" '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' ")
        else:
            past_value = json.loads(row['past_result'])
            variacion, estado_cal_var = self._calcular_variacion(present_value, past_value[0])
            if row['tipo_variacion'] in ('hist','ing'):
                df_variation = pd.DataFrame(past_value)
                df_variation.sort_index(ascending=False, inplace=True)
                df_variation['variation'] = df_variation[0].pct_change()
                avg_variation = df_variation['variation'].mean()
                stdev_variation = df_variation['variation'].std()
                cant_desv = 3
                l_range = avg_variation - (cant_desv * stdev_variation)
                r_range = avg_variation + (cant_desv * stdev_variation)

            else:
                l_range, r_range = row['param_variacion'].split('|')

            if estado_cal_var:
                if self._evaluar_variacion(l_range, r_range, variacion, past_value):
                    estado_variacion, salida = row["caso_falla"], 1
                    self.log.print_warning(" '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' ")
                    self.log.print_warning("El control {0} a nivel {1} en la tabla {2} no pasó la validacion de variacion.".format(row['ctrl'], row['nivel'], row['tabla']))
                    self.log.print_warning('El rango es de {0}|{1} pero la variacion fue de {1}'.format(l_range,r_range,variacion))
                    self.log.print_warning(" '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' ")
            else:
                estado_variacion, salida = row["caso_falla"], 1
                self.log.print_warning(" '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' ")
                self.log.print_warning("No es posible evaluar la variación del control {0} a nivel {1} en la tabla {2}.".format(row['ctrl'], row['nivel'], row['tabla']))
                self.log.print_warning("Tiene un valor pasado de 0. Se actualizara su valor presente")
                self.log.print_warning(" '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' ")

                
        return estado_variacion, salida


    def _calcular_variacion(self, present_value, past_value) -> float:
        """
        Función que calcula la variación entre el valor presente y el valor pasado del control
        Args:
            self
            present_value (float): Valor presente del control
            past_value (float): Valor pasado del control
        Returns:
            float: Variación del control
            bool: True o False
        """
        if past_value == present_value:
            return 0, True
        if past_value == 0:
            self.log.print_warning(" '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' '' ")
            self.log.print_warning(" Error al calcular la variacion. El valor pasado del control no puede ser 0 ")
            self.log.print_warning(" No se evaluará la variacion del control pero se actualizara su valor presente ")
            return 0, False
        return (float(present_value) - float(past_value)) / float(past_value), True
    
    def _evaluar_variacion(self,l_range, r_range, variacion, past_value):
        """
        Función que evalua si la variación del control se encuentra dentro del rango especificado
        Args:
            self
            l_range (float): Límite inferior del rango
            r_range (float): Límite superior del rango
            variacion (float): Variación del control
        Returns:
            bool: True o False
        """
        if l_range and r_range:
            if past_value == [-9999] or (float(l_range)<=variacion<=float(r_range)):
                return False         
        return True

    def _obtener_dato_historico(self, row, df_describe) -> str:
        '''
        Función privada que obtiene los resultados historicos del control en n ejecuciones anteriores desde la tabla
        de resultados del maestro
        Args:
            self
            row (pandas.Series): Fila del control
            df_describe (pandas.DataFrame): DataFrame de descripción de la tabla historica de resultados del maestro
        Returns:
            str: Listado de valores pasados del control en el periodo
            int: Valor -9999 cuando df_describe es vacío
        """
        '''
        if df_describe.empty:
            return str([-9999])
        periodo = row['param_variacion'].split(':')[1] if ':' in row['param_variacion'] else 1
        params = {
            'zona_tabla': f"{self.params['zona_tabla_output']}",
            'nombre_flujo': f"{row['nombre_flujo']}",
            'etapa_ejecucion': f"{row['etapa_ejecucion']}",
            'tabla': f"{row['tabla']}",
            'nom_columna': f"AND nom_columna = '{row['nom_columna']}'" if str(row['nom_columna']) != 'nan' else "",
            'ctrl': f"{row['ctrl']}",
            'parametro_ctrl': f"{row['parametro_ctrl']}",
            'periodo': f"{periodo}"
        }

        try:
            ruta_variation = "{}/sql/variation_historic.sql".format(self.ruta)
            with open(ruta_variation) as in_file_sql:
                variation_sql = in_file_sql.read()
            df_past_result = self.sp.helper.obtener_dataframe(consulta=variation_sql,params=params)
            df_past_result = df_past_result[df_past_result['parametro_ctrl']==row['parametro_ctrl']]
            df_past_result['result_ctrl'] = df_past_result['result_ctrl'].ffill().bfill()
            list_past_result = df_past_result['result_ctrl'].to_list()
        except Exception:
            raise self.log.list_error('flujos',2)
        if not list_past_result or all(past_result is None or pd.isna(past_result) for past_result in list_past_result):
            return str([-9999])
        return str(list_past_result)
    
    def _obtener_dato_tabla(self, ctrl_instance, row, tipo_variacion) -> str:
        """
        Función privada que obtiene los resultados historicos del control en n periodos anteriores desde la tabla
        original
        Args:
            self
            ctrl_instance (object): Instancia de la clase control
            row (pandas.Series): Fila del control
            tipo_variacion (str): Tipo de variación indicado para el control
        Returns:
            str: Listado de valores del control en el periodo
        """
        
        fa, filtro_p = self._construir_fechas(row, tipo_variacion)
        params = {
            'fa':fa,
            'filtro_p':filtro_p
        }

        row_var = ctrl_instance._consulta_auxiliar(row, d=2)

        try:
            df_var_result = self.sp.helper.obtener_dataframe(consulta=row_var['query'], params=params)

            if tipo_variacion == 'ing':
                df_var_result.drop(0, inplace=True)
                df_var_result.reset_index(drop=True, inplace = True)
                return str(df_var_result[df_var_result.columns[1]].to_list())
            else:
                return str(df_var_result.to_dict(orient='list'))
        except Exception:
            raise self.log.list_error('flujos',2)
       
    def _construir_fechas(self, row, tipo_variacion) -> str:
        """
        Función privada que construye los parametros fa (Fecha analisis) y filtro_p (Filtro Periodos)
        para el analisis de variación mediante ingestiones o estacionalidad
        Args:
            self
            row (pandas.Series): Fila del control
            tipo_variacion (str): Tipo de variación indicado para el control
        Returns:
            fa (str): Parametros fa construido para la consulta de variación
            filtro_p (str): Parametro filtro_p construido para la consulta de variación
        """

        columns_fa = re.search(r':(.*?)\|', row['param_variacion']).group(1)
        list_columns_fa = columns_fa.split(',')

        try:
            valores_columns_fa = []
            for column in list_columns_fa:
                valor = re.search(rf"{column}=(\d+)", row['filtro_aplicado']).group(1)
                valores_columns_fa.append(int(valor))

            valores_columns_fa.extend([1] * (3 - len(valores_columns_fa)))

            f_analisis = parser.parse(f"{valores_columns_fa[0]}-{valores_columns_fa[1]}-{valores_columns_fa[2]}")

        except:
            raise self.log.list_error('flujos',3)

        periodos = int(row['param_variacion'].split('|')[1]) if tipo_variacion == 'ing' else 60

        if len(list_columns_fa) == 1:
            fa = f'{list_columns_fa[0]} as fa'
            f_corte_y = str(f_analisis.year)
            f_corte_y_p = str((f_analisis + relativedelta(years=-periodos)).year)
            filtro_f_var = "({p_year} BETWEEN {f_corte_y_p} AND {f_corte_y})".format(campo_anio = list_columns_fa[0]
                                                                            , f_corte_y = f_corte_y
                                                                            , f_corte_y_p = f_corte_y_p
                                                                            )
        elif len(list_columns_fa) == 2:
            fa = f'{list_columns_fa[0]}*100+{list_columns_fa[1]} as fa'
            f_corte_y = str(f_analisis.year)
            f_corte_m = str(f_analisis.month)
            f_corte_y_p = str((f_analisis + relativedelta(months=-periodos)).year)
            f_corte_m_p = str((f_analisis + relativedelta(months=-periodos)).month)
            filtro_f_var = """
                    ({p_year} = {f_corte_y_p}
                    AND {p_month} BETWEEN {f_corte_m_p} AND ({f_corte_m_p}+{t})
                    )
                    OR
                    ({p_year} = {f_corte_y}
                    AND {p_month} BETWEEN ({f_corte_m}-{t}) AND {f_corte_m}
                    )
                """.format(t = periodos
                            , p_year = list_columns_fa[0]
                            , p_month = list_columns_fa[1]
                            , f_corte_y = f_corte_y
                            , f_corte_m = f_corte_m
                            , f_corte_y_p = f_corte_y_p
                            , f_corte_m_p = f_corte_m_p
                            )
        elif len(list_columns_fa) == 3:
            fa = f'{list_columns_fa[0]}*10000+{list_columns_fa[1]}*100+{list_columns_fa[2]} as fa'
            f_corte_y = str(f_analisis.year)
            f_corte_m = str(f_analisis.month)
            f_corte_d = str(f_analisis.day)
            f_corte_y_p = str((f_analisis + relativedelta(days=-periodos)).year)
            f_corte_m_p = str((f_analisis + relativedelta(days=-periodos)).month)
            f_corte_d_p = str((f_analisis + relativedelta(days=-periodos)).day)
            filtro_f_var = """
                    ({p_year} = {f_corte_y_p}
                    AND {p_month} = {f_corte_m_p}
                    AND {p_day} BETWEEN {f_corte_d_p} AND ({f_corte_d_p}+{t})
                    )
                    OR
                    ({p_year} = {f_corte_y}
                    AND {p_month} = {f_corte_m}
                    AND {p_day} BETWEEN ({f_corte_d}-{t}) AND {f_corte_d}
                    )
                """.format( t = periodos
                            , p_year = list_columns_fa[0]
                            , p_month = list_columns_fa[1]
                            , p_day = list_columns_fa[2]
                            , f_corte_y = f_corte_y
                            , f_corte_m = f_corte_m
                            , f_corte_d = f_corte_d
                            , f_corte_y_p = f_corte_y_p
                            , f_corte_m_p = f_corte_m_p
                            , f_corte_d_p = f_corte_d_p
                )
        else:
            raise self.log.list_error('flujos',4)

        return fa, filtro_f_var


    def _calcular_estacionalidad(self, row) -> int:
        """
        Función privada que calcula el valor p del Test de Kruskal para determinar la estacionalidad
        del control
        Args:
            self
            row (pandas.Series): Fila del control
        Returns:
            p (int): Valor p del Test de Kruskal
        """
        season = row['param_variacion'].split('|')[1]
        df_season = pd.DataFrame(eval(row['past_result']))

        if df_season.iloc[:,1].unique().size==1:
            return False
        
        df_season['fa'] = pd.to_datetime(df_season['fa'], format = "%Y%m%d")
        if season == 'm':
            df_season['season'] = df_season['fa'].dt.month
        elif season == 's':
            df_season['season'] = (df_season['fa'].dt.year.astype(str) + df_season['fa'].dt.week.astype(str)).astype(int)
        elif season == 'q':
            df_season.loc[df_season['fa'].dt.day <= 15, 'season'] = (df_season['fa'].dt.year.astype(str) + df_season['fa'].dt.month.astype(str) + '1').astype(int)
            df_season.loc[df_season['fa'].dt.day > 15, 'season'] = (df_season['fa'].dt.year.astype(str) + df_season['fa'].dt.month.astype(str) + '2').astype(int)
        else:
            raise self.log.list_error('flujos',5)
        
        res = [df_season.loc[df_season['season'] == i, df_season.columns[1]].values for i in df_season['season'].unique()]
        
        _, p = stats.kruskal(*res)
        
        return p
