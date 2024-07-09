#from master_validation.master_val import Master_val
from .control import Control
import pandas as pd
import time
from datetime import datetime
from vspc_config_utils.config_utils import Config_utils


class Pilar ():
    """
    Clase del maestro de validaciones. Se encarga de ejecutar la lógica para de los 3
    diferentes pilares del maestro, así como de subir los resultados del maestro a la LZ.
    """ 
    
    def __init__(self,insumos=False,
                 razonabilidad=False,
                 ejecucion=False,
                 ruta_config="",
                 df_input=pd.DataFrame(),
                 zona="",
                 tabla="",
                 ver_salida=False,
                 prueba = False,
                 columns=None,
                 logger=object,
                 periodicidad = False,
                 scrt = object,
                 anli = object,
                 variacion = False,
                 ver_filtro = False,
                 **kwargs):
        """
        Args:
            self
            zona (str, optional): Zona de la LZ donde se guardará el resultado del maestroy. Defaults to ""
            tabla (str, optional): Tabla de la LZ donde se guardará el resultado del maestro. Defaults to ""
            columns (list, optional): Listado de columnas del dataframe de controles (Inactiva). Defaults to None
            insumos (bool, optional): Activa el Pilar Insumos. Defaults to True.
            razonabilidad (bool, optional): Activa el Pilar Razonabilidad. Defaults to True.
            ejecucion (bool, optional): Activa el Pilar Ejecución. Defaults to True.
            ruta_p (str, optional): Ruta completa (Inactivo). Defaults to "".
            df_input (pandas.DataFrame, optional): Df donde se encuentran los controles a ejecutar. Defaults to pd.DataFrame().
            logger (object, optional): Instancia del logger de sparky. Defaults to object
            ver_salida (bool, optional): Activar la generación de un excel de salida por pilar (Inactivo). Defaults to False.
            prueba (bool, optional): Desactiva la subida de los resultados a la LZ. Defaults to False.
            periodicidad (bool, optional): Activa el flujo de comprobación de periodicidad. Defaults to False.
            variacion (bool, optional): Activa el flujo de comprobación de variacion de controles. Defaults to False.
            ver_filtro (bool, optional): Activa la construcción de la columna filtro_aplicado. Defaults to False.
        """
        if columns is None:
            columns = []
        #super().__init__(**kwargs)
        self.sp =kwargs.get("sparky",None)
        self.hp = self.sp.helper
        self.log = logger
        self.columns = columns
        self.ver_salida=ver_salida
        self.zona=zona
        self.tabla=tabla
        self.tiempo_default=60
        self.insumos=insumos
        self.razonabilidad=razonabilidad
        self.ejecucion=ejecucion
        self.scrt = scrt
        self.anli = anli
        self.ver_filtro = ver_filtro
        if len(df_input)>0:
            self.df_insumo=df_input
            self.estado_insumo=True
        else :
            self.estado_insumo=False#, self.df_insumo=self.ctrl.obtener_insumo("zona","tabla")
        self.fecha  = datetime.now()
        self.df_insumo = self.df_insumo.assign(
                                                f_analisis=self.fecha.date(),
                                                f_ejecucion="",
                                                n_ejecucion=0,
                                                salida=-1,
                                                estado="SIN_EJECUTAR"
                                                # ,
                                                # ingestion_day=self.fecha.day,
                                                # ingestion_month=self.fecha.month,
                                                # ingestion_year=self.fecha.year
                                                    )
        if self.df_insumo.index.empty:
            self.df_insumo  = self.df_insumo.reset_index()
        self.ctrl = Control(ruta_config=ruta_config,
                            df_input=self.df_insumo,
                            logger=self.log,
                            helper=self.sp.helper, 
                            scrt=self.scrt, 
                            anli=self.anli, variacion = variacion,
                            ver_filtro = self.ver_filtro)
        self.df_controles = pd.DataFrame()
        self.reejecucion = 0
        self.fallar = False
        self.tipo_caso=0
        self.indice=0
        self.insumo_n=0
        self.ejecucion_n=0
        self.df_resultado=pd.DataFrame()
        #self.razo_n=0
        self.prueba=prueba
        self.periodicidad = periodicidad
        self.ruta_config = ruta_config
        self.variacion = variacion
        
    
    def pilar_insumos(self,n_ejecucion=0, n=0):
        """
        Función que ejecuta la lógica del Pilar de Insumos

        Args:
            self
            n_ejecucion (int, optional): Numero de ejecuciones de la rutina (Inactivo). Defaults to 0
        Returns:
            estado (Boolean): True o False
            df (pandas.DataFrame): Dataframe con la información de los controles ejecutados
        """
        self.df_filtrado = pd.DataFrame()
        self.validar_ejecucion()
        estado = False
        df=pd.DataFrame()
        if self.insumos and len(self.df_insumo)>0 and self.insumo_n<3: 
            self.insumo_n+=1
            self.df_filtrado = self.df_insumo[(self.df_insumo['etapa_ejecucion'] == 'INSUMO') | (self.df_insumo['etapa_ejecucion'] == 'INSUMOS') ]
            if self.scrt.validar_columnas_obligatorias(df=self.df_filtrado,n='INSUMOS',i=1):
                if len(self.df_filtrado)>0:
                    estado,df = self.gestionar_controles(self.df_filtrado, n=0)

                    return estado,df
        return estado,df
    
    def pilar_razonabilidad(self, n=1):
        """
        Función que ejecuta la lógica del Pilar de Razonabilidad

        Args:
            self
        Returns:
            estado (Boolean): True o False
            df (pandas.DataFrame): Dataframe con la información de los controles ejecutados
        """
        self.df_filtrado = pd.DataFrame()
        self.validar_ejecucion()
        df=pd.DataFrame()
        estado = False
        if self.razonabilidad and len(self.df_insumo)>0 and self.reejecucion<3:
            self.df_filtrado = self.df_insumo[(self.df_insumo['etapa_ejecucion'] == 'RAZONABILIDAD') ]
            if self.scrt.validar_columnas_obligatorias(df=self.df_filtrado,n='RAZONABILIDAD',i=1):
                if len(self.df_filtrado)>0:
                    estado,df = self.gestionar_controles(self.df_filtrado, n=1)
                    return estado,df
        return estado,df
    
    def pilar_ejecucion(self,estado_logs='finalizado', n=2):
        """
        Función que ejecuta la lógica del Pilar de Ejecución

        Args:
            self
            estado_logs (str, optional): Estado de los logs de la ejecucion actual. Defaults to 'finalizado'
        Returns:
            estado (Boolean): True o False
        """
        self.validar_ejecucion()
        estado = False
        indice=0
        self.indice=indice
        self.tipo_caso=0
        if self.ejecucion and len(self.df_insumo)>0 and self.ejecucion_n<3:
            self.ejecucion_n+=1
            if self.ejecucion_n > 0:
                self.df_filtrado = self.df_insumo[(self.df_insumo['etapa_ejecucion'] == 'EJECUCION') ]
                if self.scrt.validar_columnas_obligatorias(df=self.df_filtrado,n='EJECUCION',i=1):
                    if len(self.df_filtrado)>0 and estado_logs=="finalizado":
                        indice = self.df_filtrado.index[0]
                        self.df_insumo.at[indice, "salida"]=0
                        self.df_insumo.at[indice, "estado"] = "OK"
                        estado = True
                    elif len(self.df_filtrado)>0 and estado_logs=="error":
                        indice = self.df_filtrado.index[0]
                        self.df_insumo.at[indice, "salida"]=1
                        self.df_insumo.at[indice, "estado"] = "FALLAR-REEJECUTAR"
                        self.indice, self.tipo_caso=self.validar_caso_falla(self.df_filtrado, n=2)
                        estado = True
                    else:
                        estado = False
                    #estado = self.gestionar_controles(self.df_filtrado)
                    return estado
        return estado
    
    
    def validar_df_logs(self,df_logs=pd.DataFrame()):
        """
        Función que valida el estado de los logs en una ejecución del orquestador, se utiliza
        en la lógica del Pilar Ejecución

        Args:
            self
            df_logs (pandas.DataFrame, optional): Dataframe de logs de ejecución de la rutina. Defaults to pd.DataFrame()
        Returns:
            estado (str): Estado actual de la rutina según los logs
        """
        estado="finalizado"
        df = df_logs.copy()
        df_logs_filtrado  = df[(df['estado'] != 'finalizado') & (df['etapa'] != 'Respal Logs') ]
        if len(df_logs_filtrado)> 0 :
            if (df['estado'].isin(['error', 'pendiente'])).any():
                estado="error" 
                return estado
        return estado   
            
    def gestionar_controles(self,df=pd.DataFrame(),n=0):
        """
        Función que se encarga de la construcción y ejecución de los controles definidos
        para el pilar en ejecución. Al finalizar la ejecución de los controles, se encarga
        de subir el resultado del maestro a la tabla historica en la LZ o de guardar el
        resultado en local.

        Args:
            self
            df (pandas.DataFrame, optional): Dataframe con la información de los controles a ejecutar. Defaults to pd.DataFrame()
            n (int, optional): Indicador del pilar en ejecución. Defaults to 0
        Returns:
            self.df_salida (pandas.DataFrame): Dataframe con la información de los controles ejecutados
            df_val (pandas.DataFrame): Dataframe vacío (Inactivo)
            self.estado_ctrl (Boolean): True o False
            self.validar_caso_falla(function): Ejecuta la función del mismo nombre
        """
        self.ejecucion_ctrl = False
        df_filtrado = df.copy()
        df_val = pd.DataFrame()
        if len(df_filtrado)>0:
            self.estado_ctrl=False         
            if self.periodicidad:
                df_filtrado_p = df_filtrado[(df_filtrado['periodicidad']==df_filtrado['periodo'])|(df_filtrado['periodo']==-1)]
                if df_filtrado_p.empty:
                    self.df_salida = self.anli.gestionar_periodos_nulos(df_filtrado)
                    self.df_salida = self.anli._aumentar_periodo(self.df_salida)
                    return self.validar_caso_falla(self.df_salida,n),self.df_salida
            else:
                df_filtrado_p = df_filtrado.copy()
            if self.scrt.validar_informacion_ctrl(df_filtrado_p):
                self.estado_ctrl,df_filtrado_p=self.ctrl.construir_controles(df_filtrado_p)

                self.ejecucion_ctrl,self.df_insumo=self.ctrl.ejecutar_controles(df_insumo=df_filtrado_p)
                
                if self.ejecucion_ctrl:
                    if "salida" in self.df_insumo.columns:
                        orden_columnas = self.df_insumo.columns.tolist()
                        self.df_salida = self.df_insumo.combine_first(df_filtrado)
                        if 'periodo' in self.df_salida.columns:
                            self.df_salida = self.anli._aumentar_periodo(self.df_salida)
                            orden_columnas.remove('periodo')
                            orden_columnas.append('periodo')
                        self.df_salida['f_ejecucion'] = datetime.now()
                        self.df_salida = self.df_salida[orden_columnas]
                        
                        return self.validar_caso_falla(self.df_salida,n),self.df_salida
            else:
                return self.estado_ctrl,df_val    
        return self.estado_ctrl,df_val
                    
    
    def validar_caso_falla(self,df = pd.DataFrame(),n=0):
        """
        Función que valida el caso de falla dependiendo de la información de la columna
        salida del dataframe de controles ejecutados. Genera dataframes filtrando dicha columna
        y dependiendo del tamaño de dichos dataframes ejecuta funciones para:
            reejucutar
            detener
            alertar (continuar)
            continuar

        Args:
            self
            df (pandas.DataFrame, optional): Dataframe con la información de los controles ejecutados. Defaults to pd.DataFrame()
            n (int, optional): Indicador del pilar en ejecución. Defautls to 0
        Returns:
            indice (int): Indice del control evaluado en el caso_falla
            tipo_caso (int): Tipo de caso falla del control evaluado en el caso_falla
        """
        df=df.copy()
        tipo_caso = 0
        indice = 0
        df_ree=df.loc[(df['salida'] == 1) & (df["caso_falla"] == "FALLAR-REEJECUTAR")]
        if len(df_ree)>0:
            self.indice, self.tipo_caso = self.reejecucion_caso_falla(df_ree)
            self.subirlz_df(df,n)
            return indice, tipo_caso    
        else:
            self.subirlz_df(df,n)
            self.indice = df.index[0]
            return indice, tipo_caso

    def reejecucion_caso_falla(self,df= pd.DataFrame()):
        """
        Función que maneja la lógica para la reejecución de controles en caso
        de que fallen

        Args:
            self
            df (pandas.DataFrame, optional): Dataframe con la información de los controles ejecutados. Defaults to pd.DataFrame()
        Returns:
            indice (int): Indice del control evaluado en el caso_falla
            tipo_caso (int): Tipo de caso falla del control evaluado en el caso_falla
        """
        df = df
        indice=df.index[0]
        tiempo_espera = df.at[indice,"tiempo_reejecucion"]
        tipo_caso = 1
        hora = datetime.now().strftime('%H:%M:%S') 
        if self.reejecucion <3 :
            if tiempo_espera>0:
                tiempo= tiempo_espera*60
                self.log.print_esperar(tiempo,hora)
                time.sleep(tiempo)
                self.reejecucion = self.reejecucion +1
            else:
                tiempo= self.tiempo_default*60
                self.log.print_esperar(tiempo,hora)
                time.sleep(self.tiempo_default*60) 
            return indice,tipo_caso
        else:
            self.log.print_info("ha superado el limite de reejecucion de un Control")
            return indice,tipo_caso
    
    def detener_caso_falla(self,df= pd.DataFrame()):
        """
        Función que maneja la lógica para detener el proceso en caso
        de que fallen los controles

        Args:
            self
            df (pandas.DataFrame, optional): Dataframe con la información de los controles ejecutados. Defaults to pd.DataFrame()
        Returns:
            indice (int): Indice del control evaluado en el caso_falla
            tipo_caso (int): Tipo de caso falla del control evaluado en el caso_falla
        """
        df = df
        indice =df.index[0]
        tipo_caso = 2 
        self.fallar =True
        return indice, tipo_caso
                
            # Error no hay en la columna etapa_ejecucion palabra "INSUMO"
            
    def validar_ejecucion(self):
        """
        Función que registra el numero de reejecuciones que lleva el maestro

        Args:
            self
        """
        mayor = [self.reejecucion,self.insumo_n,self.ejecucion_n]
        self.reejecucion = max(mayor)
        self.insumo_n = max(mayor)
        self.ejecucion_n = max(mayor)
        
    def subirlz_df(self,df=pd.DataFrame(),n=0):
        """
        Función que se encarga de subir los resultados de la ejecución del maestro
        a la tabla historica en la LZ, o que en su lugar guarda el archivo en local

        Args:
            self
            df (pandas.DataFrame, optional): Dataframe con la información de los controles ejecutados. Defaults to pd.DataFrame()
        Returns:
            Boolean: True o False
        """
        drop_col=["nivel",
        "campo",
        "fecha_ejecucion",
        "filtro_tabla",
        "t_n",
        "llave_primaria",
        "tipo_ctrl",
        "validar_centralizadora",
        "tiempo_validacion",
        "caso_falla",
        "tiempo_reejecucion",
        "params_control",
        "f_analisis",
        "salida",
        "ruta_sql",
        "num_params",
        "registro",
        "params_reg",
        "params_gral",
        "query",
        "year",
        "id",
        "periodicidad",
        "param_variacion",
        "variacion",
        "result_ctrl_past",
        "ingestion_year",
        "ingestion_month",
        "ingestion_day"]

        df=df.copy()
        df=df.drop(drop_col,axis=1, errors='ignore')
        c_util = Config_utils(sparky=self.sp)
        zona=self.zona
        tabla=self.tabla

        if self.prueba:
            if n==0:
                nombre_pilar = "Pilar-Insumos"
            elif n==1:
                nombre_pilar = "Pilar-Razonabilidad"
            elif n==2:
                nombre_pilar = "Pilar-Ejecucion"

            fecha_hora = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            archivo_salida = f"Salida-Prueba-{nombre_pilar}-{fecha_hora}.xlsx"
            df.to_excel(archivo_salida)
        else:
            c_util.val_hist_load_df_lz(df=df,zona_r=zona,tabla=tabla)
            return True
