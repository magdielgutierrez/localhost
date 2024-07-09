import re
import pandas as pd
import pkg_resources
import json
import os
class Seguridad():
    """
    Clase del maestro de validaciones. Se encarga de realizar validaciones varias en 
    diferentes puntos del flujo, como de columnas obligatorias, información del control
    y existencia de querys.
    """ 

    def __init__(self,  
                sparky=object, 
                logger=object,
                **kwargs):
        
        """
        Args:
            self
            sparky (object, optional): Instancia de sparky. Defaults to object
            logger (object, optional): Instancia del logger de sparky. Defaults to object
        """
        
        self.sp = sparky
        self.log = logger
        self.ruta = self._get_route()
        for key, value in kwargs.items(): 
            setattr(self, key, value)
    
    def _get_route(self):
        """
        Función Privada que arroja la ruta a la carpeta static.
        Returns:
            ruta (str): ruta de la carpeta static.
        """
        return pkg_resources.resource_filename(__name__, 'static')
       
    def validar_columnas_obligatorias(self, df=pd.DataFrame(), n='',i=0):
        """
        Función que valida que el df con controles a ejecutar contenga las filas
        obligatorias para el proceso.
        Args:
            self
            df (pandas.DataFrame, optional): Dataframe con los controles. Defaults to pd.DataFrame
            n (string, optional): Nombre del pilar en ejecución. Defaults to ''
            i (int, optional): Indicador del tipo de comportamiento de la función. Defaults to 0
        Returns:
            Boolean: True o False 
        """
    
        if i == 0:
            lista_validacion = ['nombre_flujo','etapa_ejecucion','tabla','nivel','nom_columna',
                            'filtro_tabla','t_n','tipo_ctrl','ctrl',
                            'parametro_ctrl','caso_falla','tiempo_reejecucion','accion','grp_incidente',
                            'gerencia','seccion','usuario','params_control','id']
            if set(lista_validacion).issubset(df.columns):
                return True
            else:
                raise self.log.list_error('master_val',2)

        else:
            columnas_obligatorias = {'INSUMOS': ['nombre_flujo', 'etapa_ejecucion', 'tabla', 'nivel', 'filtro_tabla', 'tipo_ctrl', 'ctrl', 'parametro_ctrl', 'caso_falla'],
                                    'EJECUCION': ['nombre_flujo', 'etapa_ejecucion', 'caso_falla'],
                                    'RAZONABILIDAD': ['nombre_flujo', 'etapa_ejecucion', 'tabla', 'nivel', 'filtro_tabla', 'tipo_ctrl', 'ctrl', 'parametro_ctrl', 'caso_falla']}
            if n not in columnas_obligatorias:
                raise ValueError(f"El valor de 'n' ({n}) no es válido.")

            valores_vacios = df[columnas_obligatorias[n]].isnull().any().any()
            if valores_vacios:
                raise self.log.list_error('pilar', 0, n)
            else:
                return True

               
    
    def validar_informacion_ctrl(self,df =pd.DataFrame):

        """
        Función que valida que el df con controles a ejecutar contenga información
        valida para la construcción de los controles. La validación la hace, principalmente,
        a partir de expresiones regulares
        Args:
            self
            df (pandas.DataFrame, optional): Dataframe con los controles. Defaults to pd.DataFrame
        Returns:
            Boolean: True o False 
        """

        with open(f'{os.path.abspath(self.ruta)}/reglas_validacion/reglas_validacion.json') as infile:
            reglas_validacion = json.load(infile)

        invalid_index = []
        if df.empty:
            raise self.log.list_error('control',3)
        else:
            for index, row in df.iterrows():
                ctrl = row['ctrl']
                nivel = row['nivel']
                param_ctrl = row['parametro_ctrl']

                if ctrl in ('tablas_p','recursos'):
                    continue

                elif re.fullmatch(r'\{([^}]*)\}',param_ctrl if ctrl == 'range' else param_ctrl.split(':')[1]):
                    continue

                elif ctrl == 'count' and nivel == 'columna':
                    sub_param_ctrl= param_ctrl.split('|')
                    for param in sub_param_ctrl:
                        name_param_ctrl = param.split(':')[0]
                        if name_param_ctrl != 'ref_val':
                            value_param_ctrl = param.split(':')[1]
                            regexp = reglas_validacion[nivel][ctrl][name_param_ctrl]
                            if self._validar_reg_exp(regexp=regexp,value=value_param_ctrl):
                                invalid_index.append(str(index))

                elif ctrl=='reg' :
                    value_param_ctrl = param_ctrl.split(':')[1]
                    try:
                        re.compile(value_param_ctrl)
                    except:
                        invalid_index.append(index)

                elif ctrl== 'range' :
                    regexp = reglas_validacion[nivel][ctrl]['range']
                    if self._validar_reg_exp(regexp=regexp,value=param_ctrl):
                        invalid_index.append(str(index))

                else:
                    name_param_ctrl = param_ctrl.split(':')[0]
                    value_param_ctrl = param_ctrl.split(':')[1]
                    regexp = reglas_validacion[nivel][ctrl][name_param_ctrl]
                    if self._validar_reg_exp(regexp=regexp,value=value_param_ctrl):
                        invalid_index.append(str(index))

            if len(invalid_index) == 0:
                return True
            else:
                invalid_ctrls = ", ".join(invalid_index)
                raise self.log.list_error('control',2, invalid_ctrls)
            
    def _validar_reg_exp (self, regexp, value):

        """
        Función privada que verifica que la información de un control siga
        una expresión regular definida
        Args:
            self
            regexp (string): Expresión regular a validar
            value (string): Valor a validar
        Returns:
            Boolean: True o False 
        """
        
        return value is None or not re.fullmatch(regexp, value)
    
    def validar_query(self, df=pd.DataFrame()):

        """
        Función que valida que el df con controles a ejecutar contenga los querys necesarios
        para la consulta de controles
        Args:
            self
            df (pandas.DataFrame, optional): Dataframe con los controles. Defaults to pd.DataFrame
        Returns:
            Boolean: True o False 
        """

        if df.empty:
            self.log.print_warning('El DataFrame está vacío.')
            return False

        if 'query' not in df.columns:
            self.log.print_warning('La columna "query" no está presente en el DataFrame.')
            return False

        if df['query'].isna().any() or (df['query'] == '').any():
            self.log.print_warning('Hay valores inválidos en la columna "query" vacíos o nulos.')
            return False
        return True
