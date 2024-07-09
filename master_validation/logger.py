


class Logger :
    def __init__(self,sparky) :
        self.sp = sparky
        self.log= self.sp.logger

    def print_info(self,text):
        """
        Función que imprime en el logger de sparky información variada
        Args:
            self
            text (str): Información a imprimir
        """
        self.sp.logger.info(msg=text)
    
    def print_warning(self,text):
        """
        Función que imprime en el logger de sparky advertencias variadas
        Args:
            self
            text (str): Advertencia a imprimir
        """
        self.sp.logger.warning(msg=text)

    def print_error(self,text):
        """
        Función que imprime en el logger de sparky errores variados y los levanta como error
        Args:
            self
            text (str): Error a imprimir
        """
        self.sp.logger.error(msg=text)
        
    
    def list_error(self,clase,index,p=''):
        """
        Función que imprime en el logger de sparky errores listados como los más comunes al momento
        de ejecutar el maestro, llamandolo desde un diccionario de errores.
        Args:
            self
            clase (str): Indica la clase del maestro donde se originó el error.
            index (int): Indicativo del id del error dentro de la clase
            p (str, optional): Complemento del mensaje de error . Default to ''
        Returns:
            errors (str): Mensaje de error extraído del diccionario errors
        """
        errors = {
            "master_val":{
                0:AttributeError("df esta vacio y zona_tabla_input no esta definida"),
                1:RuntimeError("zona_input no apunta a una zona o tabla valida"),
                2:ValueError("las columnas obtenidas para el maestro no son las predeterminadas"),
                3:ValueError("la consulta a zona_input resulto en un df vacio"),
                4:AttributeError("el parametro zona_tabla_output esta mal definido"),
                6:ValueError("variacion esta activada pero no existe dentro del df de controles")
            },
            "pilar":{
                0:ValueError("las columnas obligatorias para el pilar {0} estan incompletas o con vacios".format(p))
            },
            "control":{
                0:ValueError("el control {0} (nivel-control-parametro) no se encuentra en los registros".format(p)),
                1:RuntimeError("el control {0} fallo al ser consultado en sparky".format(p)),
                2:ValueError("los controles de los indices {0} no tienen información válida o un formato válido".format(p)),
                3:ValueError("el df de controles está vacío"),
                4: ValueError("el control # {0} de consumo de recursos no se puede ejecutar debido a que la ruta al archivo sql no existe".format(p))
            },
            "query":{
                0:ValueError("el control {0} no existe y por tanto no se puede construir el query".format(p)),
                1:ValueError("el parametro indicado para el control no existe y por tanto no se puede construir el query".format(p))
            },
            "flujos":{
                0:ValueError("periodicidad esta activada pero no existe dentro del df de controles"),
                1:ValueError("variacion esta activada pero no está definido el tipo_variacion para los controles"),
                2:RuntimeError("variacion historica falló al ser consultada en sparky"),
                3:RuntimeError("fallo al construir las fechas de analisis para la variacion historica"),
                4:ValueError("param_variacion tiene mas de 3 columnas para definir las fechas de analisis"),
                5:ValueError("el tipo de estacionalidad indicado no hace parte de los tipos permitidos"),
            }
        }
        self.sp.logger.error(msg=errors[clase][index])
        return errors[clase][index]
        
    def print_pilar_i(self,pilar):
        """
        Función que imprime en el logger de sparky un mensaje relativo al inicio de
        ejecución de un pilar del maestro de validaciones
        Args:
            self
            pilar (int): Número indicativo del pilar en ejecución.
        """
        pilar = pilar
        nom_pilar=""
        if pilar ==1:
            nom_pilar = "INSUMO"
        elif pilar == 2:
            nom_pilar = "EJECUCION"
        elif pilar == 3:
            nom_pilar = "RAZONABILIDAD"

        self.sp.logger.info(msg='\n \n**  **  **  **  **  **  **  **  **  **  **  **  **  **  **  **  **  **  **  **  **  **  ** ')
        self.sp.logger.info(msg='     INICIA Maestro Validaciones -- Pilar {}        '.format(nom_pilar))
        self.sp.logger.info(msg=' --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  -- ')
        
    def print_reejecucion(self,pilar):
        """
        Función que imprime en el logger de sparky un mensaje relativo a la
        reejecución de un pilar del maestro de validaciones
        Args:
            self
            pilar (int): Número indicativo del pilar en ejecución.
        """
        pilar = pilar
        nom_pilar=""
        if pilar ==1:
            nom_pilar = "INSUMO"
        elif pilar == 2:
            nom_pilar = "EJECUCION"

        self.sp.logger.info(msg=' --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  -- ')
        self.sp.logger.info(msg=' --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  -- ')
        self.sp.logger.info(msg='     REEJECUCION Maestro Validaciones -- Pilar {}        '.format(nom_pilar))
        self.sp.logger.info(msg=' --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  -- ')
        self.sp.logger.info(msg=' --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  -- ')

    def print_fin(self,pilar):
        """
        Función que imprime en el logger de sparky un mensaje relativo al final de
        ejecución de un pilar del maestro de validaciones
        Args:
            self
            pilar (int): Número indicativo del pilar en ejecución.
        """
        pilar = pilar
        nom_pilar=""
        if pilar ==1:
            nom_pilar = "INSUMO"
        elif pilar == 2:
            nom_pilar = "EJECUCION"
        elif pilar == 3:
            nom_pilar = "RAZONABILIDAD"

        self.sp.logger.info(msg=' --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  -- ')
        self.sp.logger.info(msg='     FINALIZA Maestro Validaciones -- Pilar {}        '.format(nom_pilar))
        self.sp.logger.info(msg=' **  **  **  **  **  **  **  **  **  **  **  **  **  **  **  **  **  **  **  **  **  **  ** ')
        
    def print_esperar(self,tiempo,hora):
        """
        Función que imprime en el logger de sparky un mensaje relativo a la congelación
        de la rutina del maestro de validaciones
        Args:
            self
            tiempo (int): Número de segundos que la rutina estará congelada
            hora (int): Hora actual de la ejecución
        """
        tiempo = tiempo
        hora=hora
        self.sp.logger.print(
                    '\n \n --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  -- ')
        self.sp.logger.print(
                    ' hora:  {}    RUTINA CONGELADA:  {} - Minutos       \n \n'.format(hora,tiempo/60))
        self.sp.logger.print(
                    ' --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  -- \n \n')
        
    def print_controles(self):
        """
        Función que imprime en el logger de sparky un mensaje relativo al inicio de
        del paso de obtención de controles por parte del maestro
        Args:
            self
        """

        self.sp.logger.info(msg=' --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  -- ')
        self.sp.logger.info(msg=' Maestro Validaciones - Obtener CONTROLES')
        self.sp.logger.info(msg=' --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  -- \n \n')

    def print_periodos(self):
        """
        Función que imprime en el logger de sparky un mensaje relativo al inicio de
        del paso de obtención de periodos por parte del maestro
        Args:
            self
        """

        self.sp.logger.info(msg=' --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  -- ')
        self.sp.logger.info(msg=' Maestro Validaciones - Obtener PERIODOS')
        self.sp.logger.info(msg=' --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  -- \n \n')

    def print_variacion(self):
        """
        Función que imprime en el logger de sparky un mensaje relativo al inicio de
        del paso de obtención de variacion de controles por parte del maestro
        Args:
            self
        """

        self.sp.logger.info(msg=' --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  -- ')
        self.sp.logger.info(msg=' Maestro Validaciones - Obtener VARIACION')
        self.sp.logger.info(msg=' --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  -- \n \n')

    def print_fallo_control(self, control, nivel, tabla, detalle, query, columna = ''):
        """
        Función que imprime en el logger de sparky un mensaje relativo al fallo de
        ejecución de un control del maestro
        """
        tabla_col = (tabla + columna) if columna else tabla
        self.sp.logger.warning(msg=' --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  ')
        self.sp.logger.warning(msg='''El control {0} a nivel {1} en la tabla {2} no paso. \n
                                    {3}\n
                                    Puedes comprobar esta información ejecutando el siguiente query:\n
                                    {4}'''. format(control, nivel, tabla_col, detalle, query))
        self.sp.logger.warning(msg=' --  --  --  --  --  --  --  --  --  --  --  --  --  --  --  ')