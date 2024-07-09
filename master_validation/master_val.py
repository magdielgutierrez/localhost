import pandas as pd
import pkg_resources

from .pilar import Pilar
from .logger import Logger
from .seguridad import Seguridad 
from .analisis import Analisis
from .utils import Utils


class Master_val():

    """
    Clase inicial del maestro de validaciones. Se encarga de obtener los controles,
    la periodicidad, iniciar el constructor de sparky y llamar al pilar correspondiente.
    """

    def __init__(self, 
                 zona_tabla_output,
                 zona_tabla_input="",
                 insumos=True,
                 razonabilidad=True,
                 ejecucion=True,
                 activar=True,
                 ruta_config="",
                 df_input=pd.DataFrame(),
                 sparky=None,
                 username="",
                 ver_salida=False,
                 prueba=False,
                 periodicidad=False,
                 variacion = False,
                 ver_filtro = False):
                 
        """
        Args:
            zona_tabla_input (str): Zona de la LZ donde se encuentran los controles a ejecutar
            zona_tabla_output (str): Zona de la LZ donde se guardará el resultado del maestro
            insumos (bool, optional): Activa el Pilar Insumos. Defaults to True.
            razonabilidad (bool, optional): Activa el Pilar Razonabilidad. Defaults to True.
            ejecucion (bool, optional): Activa el Pilar Ejecución. Defaults to True.
            activar (bool, optional): Activa el maestro de validaciones. Defaults to True.
            ruta_p (str, optional): Ruta completa (Inactivo). Defaults to "".
            df_input (pandas.DataFrame, optional): Df donde se encuentran los controles a ejecutar. Defaults to pd.DataFrame().
            sparky (object, optional): Instancia de Sparky. Defaults to None.
            username (str, optional): Nombre de Usuario de red para Sparky. Defaults to "".
            ver_salida (bool, optional): Activar la generación de un excel de salida por pilar (Inactivo). Defaults to False.
            prueba (bool, optional): Desactiva la subida de los resultados a la LZ. Defaults to False.
            periodicidad (bool, optional): Activa el flujo de comprobación de periodicidad. Defaults to False.
            variacion (bool, optional): Activa el flujo de comprobación de variacion de controles. Defaults to False.
            ver_filtro (bool, optional): Activa la construcción de la columna filtro_aplicado. Defaults to False.
        """
        self.zona_tabla_input=zona_tabla_input
        self.zona_tabla_output=zona_tabla_output
        self.insumos=insumos
        self.ver_salida = ver_salida
        self.razon = razonabilidad
        self.ejecu=ejecucion
        self.ruta_config=ruta_config
        self.activar=activar
        self.sp = sparky
        self.cols = [
            "params_control",
            "caso_falla",
            "ruta_sql",
            "num_params",
            "registro",
            "params_reg",
            "query",
            "params_gral",
            "salida",
            "estado_control"]
        if self.sp != None:
            self.log=Logger(self.sp)
        self.username=username
        self.df_controles=df_input
        self.df_controles_original = df_input
        self.pl=None
        self.prueba=prueba
        self.periodicidad=periodicidad
        self.variacion = variacion
        self.df_periodo = pd.DataFrame()

        self.anli = Analisis(sparky=self.sp,logger=self.log, params=self.__dict__)
        self.ver_filtro = ver_filtro
        
        if self._sparky_controles():
            if self.periodicidad:
                self.df_controles = self.anli.gestionar_periodos()
        
        
    def _sparky_controles(self):
        """
        Función privada que inicializa sparky y valida si existe un dataframe de insumo vacio o lleno , 
        valida si activar es True o False 
        True si inicializacia bien de los contrario False
        Args:
            self
        Returns:
            Boolean: True o False 
        """
        if (len(self.df_controles)>0 and self.activar) or (len(self.df_controles)==0 and self.verificar_params() and self.activar):
            if self.contructor_sparky():
                if self.obtener_controles():
                    return True
        return False
    
    def _pilar_validacion(self,pilar=0):
        """
        Función privada que instancia la clase pilar según los parametros ingresados por el usuario
        y el método de pilar que invoque
        Args:
            self
            pilar (int, optional): Indica que pilar será instanciado. Defaults to 0 
        """
        zot = self.zona_tabla_output.split(".")
        if len(zot)==2:
            zona=zot[0]
            tabla =zot[1]
            self.pl = Pilar(ruta_config=self.ruta_config,
                            df_input=self.df_controles,
                            sparky=self.sp,
                            zona=zona,
                            tabla=tabla,
                            ver_salida=self.ver_salida,
                            columns=self.cols,
                            logger=self.log,
                            prueba=self.prueba,
                            periodicidad = self.periodicidad,
                            scrt=self.scrt,
                            anli = self.anli,
                            variacion = self.variacion,
                            ver_filtro = self.ver_filtro)
            if pilar == 1:
                self.pl.insumos=True
            if pilar == 2:
                self.pl.ejecucion=True
            if pilar == 3:
                self.pl.razonabilidad=True
        else:
            raise self.log.list_error('master_val',4)
            
    
    def ejecucion(self,orquestador_dos = object,df_logs=pd.DataFrame()):
        """
        Función que ejecuta la lógica del Pilar Ejecución del maestro de validaciones.
        Es llamada directamente por el usuario para ejecutar el pilar en el código
        Args:
            self
            orquestador_dos (object, optional): Instancia del orquestador dos. Defaults to object
            df_logs (pandas.DataFrame, optional): Df que almacena los logs del orquestador. Defaults to pd.DataFrame()
        Returns:
            Boolean: True o False 
        """
        orquestador = orquestador_dos
        if self.ejecu:
            #if len(orquestador)>0:
                orquestador.ejecutar()
                df_logs = orquestador.sparky.logger.df
                if len(df_logs)>0:
                    #if self._sparky_controles():
                        #self.log = Logger(self.sp)
                    self.log.print_pilar_i(2)
                    self._pilar_validacion(pilar=2)
                    estado_logs = self.pl.validar_df_logs(df_logs=df_logs)
                    self.caso_falla = "INICIAR"
                    while ((self.caso_falla != "FALLAR" and self.caso_falla != "CONTINUAR") or self.caso_falla == "FALLAR-REEJECUTAR") and self.pl.reejecucion < 3 : 
                        if  self.ejecu and self.pl.ejecucion:
                            self.pl.pilar_ejecucion(estado_logs=estado_logs)
                            if self.pl.tipo_caso > 0:
                                if self.pl.tipo_caso==1:
                                    self.caso_falla = "FALLAR-REEJECUTAR"
                                    orquestador.ejecutar()
                                    df_logs_orquestador=orquestador.sp.logger.df
                                    df_logs_copy = df_logs.copy()
                                    estado_logs = self.pl.validar_df_logs(df_logs=df_logs_orquestador)
                                    
                                if self.pl.tipo_caso==2:
                                    self.caso_falla = "FALLAR"
                                    return False
                            else:
                                self.caso_falla = "CONTINUAR"
                    self.log.print_fin(2)
                    return self.detener_caso_falla("EJECUCIóN")
                    #self.log.print_fin(2)
            #return False
        else:
            return True
    
    
    def insumo(self):
        """
        Función que ejecuta la lógica del Pilar Insumos del maestro de validaciones.
        Es llamada directamente por el usuario para ejecutar el pilar en el código
        Args:
            self
        Returns:
            Boolean: True o False 
        """
        estado=False
        if self.insumos:
            df = pd.DataFrame()
            #if self._sparky_controles():
            self.log.print_pilar_i(1)
            self._pilar_validacion(pilar=1)
            n_ejecucion=0
            self.caso_falla = "INICIAR"
            while ((self.caso_falla != "FALLAR"  and self.caso_falla != "CONTINUAR") or self.caso_falla == "FALLAR-REEJECUTAR" )  and self.pl.reejecucion < 3 :
                n_ejecucion+=1
                if n_ejecucion > 1:
                    self.log.print_reejecucion(1)
                if  self.insumos and self.pl.insumos:
                    estado,df = self.pl.pilar_insumos(n_ejecucion)
                    df_sal = df.loc[df['salida'] == 1]
                    if estado and len(df_sal)==0:
                        self.caso_falla = "CONTINUAR"
                    elif len(df_sal) >0 :
                        # Applymap genera un warning, se puede reemplazar por df_sal.map
                        # funciona exactamente igual. Pero depende de la version de pandas
                        # probablemente se elimine en futuras versiones
                        if df_sal.applymap(lambda x: x == 'FALLAR').any().any():
                            self.caso_falla = "FALLAR"
                        elif df_sal.applymap(lambda x: x == 'FALLAR-REEJECUTAR').any().any():
                            self.caso_falla = "FALLAR-REEJECUTAR"
                        else:
                            self.caso_falla = "CONTINUAR"
            self.log.print_fin(1)
            return self.detener_caso_falla("INSUMO")
            #return estado
        else:
            estado = True
            return estado

    def razonabilidad(self):
        """
        Función que ejecuta la lógica del Pilar Razonabilidad del maestro de validaciones.
        Es llamada directamente por el usuario para ejecutar el pilar en el código
        Args:
            self
        Returns:
            Boolean: True o False 
        """
        #if self._sparky_controles():
        #self.log = Logger(self.sp)
        self.log.print_pilar_i(3)
        self._pilar_validacion(pilar=3)
        if  self.razon:
            estado,_ = self.pl.pilar_razonabilidad()
            if estado :
                self.caso_falla = "CONTINUAR"
                self.log.print_fin(3)
                return True
        self.log.print_fin(3)
        return False
    
    def verificar_params(self):
        """
        Función que verifica que los parametros introducidos para zona_tabla_input y
        zona_tabla_output tengan la estructura necesaria de una tabla en la LZ
        Args:
            self
        Returns:
            Boolean: True o False 
        """
        if  ("." not in self.zona_tabla_input) and ("." not in self.zona_tabla_output) :
            return False
        if len(self.zona_tabla_input)<9 and len(self.zona_tabla_output)<9:
            return False
        return True
    
    def contructor_sparky(self,orquestador = object()):
        """
        Función que verifica si existe una instancia de Sparky enviada como parametro al
        momento de instanciar el maestro. En caso negativo, recibe una instancia de orquestador
        para obtener su sparky. Finalmente genera una instancia de la clase Logger y de la clase
        Seguridad.
        Args:
            self
            orquestador (object, optional): Instancia del orquestador dos. Defaults to object
        Returns:
            Boolean: True o False 
        """
    
        estado = False
        if type(orquestador)==object:
            if self.sp !=None:
                self.log = Logger(self.sp)
                estado = True
            else:
                self.log.print_error("No se ha inicializado sparky") 
    
        else:
            self.sp=orquestador.sparky
            estado = True
        
        self.scrt = Seguridad(sparky=self.sp,logger=self.log)
        return estado
    
    def obtener_controles(self):
        """
        Función que obtiene el listado de controles a ejecutar. En caso de que los controles
        se pasen con el argumento zona_tabla_input la función generará una consulta a la 
        última ingestión de la tabla, finalmente realizando una transformación al dataframe 
        obtenido. Si por el contrario los controles se pasaron con el argumento df_input,
        la función solo realizara la transformación de datos.
        Args:
            self
        Returns:
            Boolean: True o False 
        """
        estado = False
        params={}
        self.df_controles = self.df_controles_original
        if self.df_controles.empty and (self.zona_tabla_input == ""):
            raise self.log.list_error('master_val',0)
        elif self.df_controles.empty:
            query_describe = "DESCRIBE {}".format(self.zona_tabla_input)
            self.log.print_controles()
            try:
                df_describe=self.sp.helper.obtener_dataframe(query_describe)
            except:
                raise self.log.list_error('master_val',1)
            params["columns_name"] = Utils._columns_namestypes(df_describe)
            params["zona_tabla"] = self.zona_tabla_input
            ruta_select = "{}/sql/select.sql".format(pkg_resources.resource_filename(__name__, 'static'))
            with open(ruta_select) as in_file_sql:
                select_sql = in_file_sql.read()
            self.df_controles = self.sp.helper.obtener_dataframe(consulta=select_sql,params=params)
            self.df_controles["id"]=range(0, len(self.df_controles))
            self.df_controles= Utils.eliminar_columnas(self.df_controles)
            if len(self.df_controles)>0:
                self.df_controles = self.df_controles.assign(cols="")
                self.df_controles = self.df_controles.rename(columns={'cols': 'params_control'})
                if self.df_controles['params_control'].isna().sum() > 1:
                    self.df_controles=self.df_controles['params_control'].fillna("", inplace=True)
            else:
                raise self.log.list_error('master_val',3)
        else:
            val_cols = set(self.cols) - set(self.df_controles.columns)
            self.df_controles= Utils.eliminar_columnas(self.df_controles)
            if val_cols:
                self.df_controles = self.df_controles.assign(cols="")
                self.df_controles = self.df_controles.rename(columns={'cols': 'params_control'})
                if self.df_controles['params_control'].isna().sum() > 1:
                    self.df_controles=self.df_controles['params_control'].fillna("", inplace=True)
                self.df_controles["id"]=range(1, len(self.df_controles)+1)
        
        if self.scrt.validar_columnas_obligatorias(df = self.df_controles):
            estado = True

        return estado
    
    def detener_caso_falla(self,pilar=""):
        """
        Función que valida la fila caso_falla de cada control y ejecuta la acción correspondiente
        dependiendo de la información suministrada en el control
        Args:
            self
            pilar (str, optional): Indica el pilar sobre el cual se esta trabajando. Defaults to ""
        Returns:
            Boolean: True o False
        """
        if self.pl !=None:
            if self.caso_falla == "FALLAR":
                self.sp.logger.error("ERROR: Uno de los controles Fallo y caso falla es FALLAR se detuvo la ejecucion")
                return False
            elif self.pl.reejecucion>=3 and self.caso_falla == "FALLAR-REEJECUTAR":
                self.sp.logger.error("ERROR: Uno de los controles Fallo y alcanzo el maximo de reejecuciones se detuvo la ejecucion")
                return False
            self.sp.logger.info("****************** EL PILAR {} SE EJECUTO CORRECTAMENTE *********************".format(pilar))
            return True
        