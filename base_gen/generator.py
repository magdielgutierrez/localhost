from cookiecutter.main               import cookiecutter
from prettytable                     import PrettyTable
from datetime	       		         import datetime
import pkg_resources
import json
import os
import re
import subprocess

class GeneratorManager(): 
    """
    Clase encargada del manejo y control
    de la interfaz de usuario via consola
    para la generacion automatica de templates
    basandose en la estructura del orquestador2
    """
    def __init__(self, ruta=False ,nombre=False , tipo=False , indice=False, author = False, venv = False, lib = False):
        """
        Constructor de la clase GeneratorManager
        ------
        Return
        ------
        generator : GeneratorManager
        Retorna una nueva instancia de un objeto
        de la clase
        """
        self.params_cookiecutter = {}
        self._template = ""
        self.ruta = ruta
        self.nombre = nombre
        self.tipo = tipo
        self.indice = indice
        self.author = author
        self.venv = venv
        self.lib = lib

    @staticmethod
    def _get_templates_path():
        """
        Funcion encargada de identificar la
        carpeta donde se almacenan los
        templates almacenados
        ------
        Return
        ------
        ruta_src : string
        Ruta static en el sistema o entorno de
        los recursos del paquete
        """
        return pkg_resources.resource_filename(__name__, 'templates')

    def _clear_text(self, value, first_character=True, last_character=True):
        """
        Función encargada de realizar
        una limpieza básica del mismo
        ---------
        Parameters
        ---------
        field : str
        Texto al cual se le desea aplicar
        el proceso de limpieza

        first_character : Bool, default = True, opcional
        Bandera que indica si se deben
        limpiar caracteres al inicio del texto

        last_character : Bool, default = True, opcional
        Bandera que indica si se deben
        limpiar caracteres al final del texto
        ------
        Return
        ------
        clean_value : str
        Texto después de haber sido
        aplicado el proceso de limpieza
        """
        clean_value = re.sub('-+', '-', value)
        clean_value = re.sub('_+', '_', clean_value)
        clean_value = re.sub(' +', ' ', clean_value)
        if first_character:
            clean_value = clean_value.lstrip(" -_")
        if last_character:
            clean_value = clean_value.rstrip(" -_")
        return clean_value

    def _default_params(self):
        self.params_cookiecutter["creation_date"]       = datetime.today().strftime("%Y%m%d")
        self.params_cookiecutter["dsn_name"]            = "impala-virtual-prd"
        self.params_cookiecutter["python_version"]      = "3.9"
        self.params_cookiecutter["step1_file_name"]     = "etl"
        self.params_cookiecutter["step1_class_name"]    = "ETL"

    def _manage_answer(self):
        pass


    @staticmethod
    def _print_menu(options):
        """
        Funcion encargada de mostrar en
        consola una lista de opciones para
        seleccionar en un formato ordenado
        como tabla utilizando la libreria
        PrettyTable
        ---------
        Parameters
        ---------
        options : dict
        Diccionario con la lista de opciones
        y algunos parametros de configuracion
        permitidos para la visualizacion de la
        informacion en consola
        """
        t = PrettyTable(options["headers"])
        for item in options["rows"]:
            t.add_row(item)
        t.title = options["title"]
        t.align = options["align"]
        print(t)
    
    def _step_manager(self):
        """
        Funcion encargada de invocar
        la solicitud de parametros al
        usuario via consola
        """
        steps = [
            self._set_vp,
            self._set_repo,
            self._set_output_path,
            self._set_author,
            self._set_venv,
            self._set_process_zone,
            self._set_index,
            self._set_type
        ]

        self.screen = 0
        
        for step_function in steps:
            self._clear_console(self.screen)
            step_function()
            self.screen = self.screen + 1
            

    def _set_vp(self): 
        self.params_cookiecutter["vp"]     = 19
        self.params_cookiecutter["vp_min"] = "vspc"
        self.params_cookiecutter["vp_max"] = "Vicepresidencia de Servicios para los Clientes"

    def _set_repo(self): 
        """
        Funcion encargada de configurar las variables 
        requeridas para la parametrización de la platilla 
        """
        user_input= self._manage_repo(self.nombre)
        self.params_cookiecutter["repo"]          = self._clear_text(user_input)
        self.params_cookiecutter["package_name"]  = self.params_cookiecutter["repo"]
        self.params_cookiecutter["name"]          = self.params_cookiecutter["repo"].split("-")[2]
        self.params_cookiecutter["src_path"]      = self.params_cookiecutter["repo"].replace("-", "_")
        self.params_cookiecutter["package_title"] = "Orq 2 {}".format(
            user_input.replace("-", " ")
        )

    def _manage_repo(self, user_entry): 
        """
        Funcion encargada del manejo
        y control de excepciones
        para el campo nombre del repositorio
        ---------
        Parameters
        ---------
        user_entry : str
        Nombre para solicitar el campo
        al usuario
        ------
        Return
        ------
        user_entry : str
        Nombre del repositorio ingresado
        por el usuario
        """
        while True:
            if not user_entry:
                options = {
                "title"     : ""
                , "align"   : "l"
                , "headers" : ["Condiciones del campo"]
                , "rows"    : [
                    ["El nombre del repositorio no debe contener espacios"]
                    , ["El separador debe ser guión medio (-)"]
                    , ["Los caracteres especiales serán ignorados"]
                    , ["El texto se tomará completamente en minúscula"]
                    , ["El nombre debe tener mínimo 10 caracteres"]
                    , ["El nombre no debe incluír multiples guiones continuos"]
                    ]
                }
                self._print_menu(options)
                user_entry = str(input("{}: ".format("Ingrese el nombre del proyecto (repositorio)")))
                
            user_entry_clean = user_entry.lower().replace("_", "-")
            user_entry_clean = user_entry_clean.replace(" ", "-")
            user_entry_clean = re.sub('[^A-Za-z-]+', '', user_entry_clean)
            
            if user_entry_clean == "":
                user_entry = False
                print("Debe ingresar un nombre valido")
                continue
            if len(user_entry_clean) <=10:
                user_entry = False
                print("La cantidad de caracteres sin el acrónimo debe ser mayor o igual a 10")
                continue
            if len(user_entry_clean) >=35:
                user_entry = False
                print("La cantidad de caracteres sin el acrónimo debe ser menor o igual a 35")
                continue
            if len(user_entry_clean.split("-")) != 3:
                user_entry = False
                print("* No cumple con los estándares para nombre de repositorios. ejemplo: vspc-gis-tipo")
                continue
            break
        return user_entry_clean

    def _set_output_path(self): 
        """
        Funcion encargada de configurar la ruta de salida
        ingresada por el usuario
        """
        user_input = self._manage_output_path(self.ruta)
        if user_input != "":
            self.params_cookiecutter["output_path"] = user_input
        else:
            self.params_cookiecutter["output_path"] = os.getcwd()

    def _manage_output_path(self, user_entry): 
        """
        Funcion encargada del manejo
        y control de excepciones
        para la ruta de generacion
        ---------
        Parameters
        ---------
        user_entry : str
        Texto para solicitar el campo
        al usuario
        ------
        Return
        ------
        path : str
        Ruta para almacenar la
        estrcutura de archivos generada
        """
        path = ""
        while True:
            if not user_entry:
                options = {
                "title"     : ""
                , "align"   : "l"
                , "headers" : ["Condiciones del campo"]
                , "rows"    : [
                    ["La ruta debe ser asociada a una carpeta"]
                    , ["Si ya existe el repositorio en la ruta no se reemplazará"]
                    ]
                }
                self._print_menu(options)
                user_entry = input("{}: ".format("Ingrese la ruta para guardar el repositorio generado"))
            path = user_entry
            if path != "" and os.path.exists(os.path.join(path, self.params_cookiecutter["repo"])):
                user_entry = False
                print("La ruta del repositorio a generar ya existe, debe ingresar una ruta valida")
                continue
            if path != "" and not os.path.exists(path):
                user_entry = False
                print("La ruta raiz no existe, debe ingresar una ruta valida")
                continue
            break
        return path

    def _set_description(self):
        """
        Funcion encargada de configurar la descripción
        por defecto
        """
        self.params_cookiecutter["package_description"] = ""

    def _manage_author(self, user_entry):
        """
        Funcion encargada del manejo
        y control de excepciones
        para el campo autores
        ---------
        Parameters
        ---------
        msg : str
        Texto para solicitar el campo
        al usuario
        ------
        Return
        ------
        user_entry : str
        Autores del paquete ingresados
        por el usuario
        """
        while True:
            if not user_entry:
                options = {
                    "title"     : ""
                    , "align"   : "l"
                    , "headers" : ["Condiciones del campo"]
                    , "rows"    : [
                        ["Debe ingresar el usuario de red de los autores separados por coma ','"]
                        , ["Los espacios o caracteres adicionales a letras serán escapados"]
                        , ["Se permiten máximo 3 autores"]
                        , ["El primer o único autor, será tomado para el correo electrónico registrado en la rutina"]
                    ]
                }
                self._print_menu(options)
                user_entry = str(input("{}: ".format("Ingrese el o los autores del paquete")))
            user_entry_clean = user_entry.replace(";", ",")
            user_entry_clean = re.sub('[^A-Za-z,]+', '', user_entry).lower()
            if len(user_entry_clean.split(",")) > 3:
                user_entry = False
                print("Solamente se permiten hasta 3 usuarios como autores")
                continue
            if user_entry_clean == "":
                user_entry = False
                print("Debe ingresar al menos un autor")
                continue
            break
        return user_entry_clean
    
    def _set_author(self):
        user_input = self._manage_author(self.author)
        self.params_cookiecutter["package_author_email"] = "{}@bancolombia.com.co".format(
            user_input.lower().split(",")[0]
        )
        self.params_cookiecutter["package_author"] = user_input.lower()

    def _set_process_zone(self):
        """
        Funcion encargada de configurar la zona de proceso
        por defecto
        """
        self.params_cookiecutter["process_zone"] = "proceso"

    def _set_index(self): 
        """
        Funcion encargada de configurar el indice de la plantilla 
        ingresado por el usuario
        """
        user_input= self._manage_index(self.indice)
        self.params_cookiecutter["index"] = user_input
        self.params_cookiecutter["excel"]         = "{}_ctrl_masterval_{}".format(self.params_cookiecutter["index"],self.params_cookiecutter["name"])
        
    def _manage_index(self, user_entry): 
        """
        Funcion encargada del manejo
        y control de excepciones
        para el campo indice del repositorio
        ---------
        Parameters
        ---------
        user_entry : str
        Nombre para solicitar el campo
        al usuario
        ------
        Return
        ------
        user_entry : str
        Nombre del repositorio ingresado
        por el usuario
        """
        while True:
            if not user_entry:
                options = {
                    "title"     : ""
                    , "align"   : "l"
                    , "headers" : ["Condiciones del campo"]
                    , "rows"    : [
                        ["El indice debe contener solamente letras y se permite guión bajo como separador"]
                        , ["La idea del indice es que permita identificar de una manera simple las tablas relacionadas"]
                        , ["El tamaño máximo es de 12 caracteres"]
                    ]
                }
                self._print_menu(options)
                user_entry = str(input("{}: ".format('Ingrese el indice para nombramiento de tablas')))
            user_entry_clean = user_entry.lower().replace("-", "")
            user_entry_clean = user_entry_clean.lower().replace("_", "")
            user_entry_clean = user_entry_clean.replace(" ", "")
            user_entry_clean = re.sub('[^A-Za-z]+', '', user_entry_clean)

            if len(user_entry_clean) > 12:
                print("El tamaño maximo es de 12 caracteres")
                user_entry = False
                continue
            if user_entry_clean == "":
                user_entry = False
                print("El prefijo para las tablas es un campo obligatorio")
                continue
            break
        return user_entry_clean

    def _manage_lib(self,user_entry):
        while True:
            if not user_entry:
                options = {
                    "title"     : ""
                    , "align"   : "l"
                    , "headers" : ["Condiciones del campo"]
                    , "rows"    : [
                        ["Indica con un 1 si deseas que el generador instale las librerías default del repositorio"]
                        ,["Indica con un 0 si deseas continuar sin instalar las librerías"]
                        ,["Ten en cuenta que debes tener correctamente configurado artifactory en tu PC"]
                    ]
                }
                self._print_menu(options)
                user_entry = str(input("{}: ".format('¿Deseas instalar las librerías default?')))
            try:
                user_entry_clean = int(user_entry)
                if user_entry_clean == 0:
                    user_entry_clean = False
                    break
                elif user_entry_clean == 1:
                    user_entry_clean = True
                    break
                else:
                    user_entry = False
                    print("No ingresaste una opción válida")
                    continue
            except:
                user_entry = False
                print("No ingresaste una opción válida")
                continue
            
        return user_entry_clean

    def _manage_venv(self, user_entry):
        while True:
            if not user_entry:
                options = {
                    "title"     : ""
                    , "align"   : "l"
                    , "headers" : ["Condiciones del campo"]
                    , "rows"    : [
                        ["Indica con un 1 si deseas que el generador cree el ambiente virtual para tu repositorio"]
                        ,["Indica con un 0 si deseas continuar sin crear el ambiente virtual"]
                    ]
                }
                self._print_menu(options)
                user_entry = str(input("{}: ".format('¿Deseas crear el ambiente virtual?')))
            try:
                user_entry_clean = int(user_entry)
                if user_entry_clean == 0:
                    user_entry_clean = False
                    user_entry_lib = False
                    break
                elif user_entry_clean == 1:
                    user_entry_clean = True
                    user_entry_lib = self._manage_lib(self.lib)
                    break
                else:
                    user_entry = False
                    print("No ingresaste una opción válida")
                    continue
            except:
                user_entry = False
                print("No ingresaste una opción válida")
                continue
            
        return user_entry_clean, user_entry_lib
    
    def _set_venv(self):
        self.user_venv,self.user_lib = self._manage_venv(self.venv)

    def _manage_type(self, user_entry): 
        """
        Funcion encargada del manejo
        y control de excepciones
        para el campo vicepresidencia
        ---------
        Parameters
        ---------
        msg : str
        Texto para solicitar el campo
        al usuario
        ------
        Return
        ------
        user_entry : int
        Id de la opcion seleccionada por
        el usuario
        """
        options = {
                    "title"     : ""
                    , "align"   : "l"
                    , "headers" : ["Condiciones del campo"]
                    , "rows"    : [
                        ["Debe ingresar el indice relativo al template a generar"]
                        , ["1: Template ETL"]
                        , ["2: Template Modelo"]
                        , ["3: Template Centralizador"]
                        , ["4: Template Formacion"]
                    ],
                    "options":["vspc/template-etl-orq2"
                        , "vspc/template-model-orq2"
                        , "vspc/template-centralizador-orq2"
                        , "vspc/template-formacion-orq2"
                    ]
                }
        while True:
            if not user_entry:
                self._print_menu(options)
                user_entry = str(input("{}: ".format('Ingrese el indice relativo a el template a generar')))
            user_entry_int = int(user_entry)
            if user_entry_int < 1 and user_entry_int <5:
                user_entry = False
                print("La opción es inválida")
                continue
            break
        return  options["options"][user_entry_int - 1]
    
    def _set_type(self):
        """
        Funcion encargada de configurar el tipo de plantilla
        ingresada por el usuario
        """  
        user_input = self._manage_type(self.tipo)
        self._template = user_input
        

    def _create_virtualenv(self):
        project_dir = os.path.join(
                    self.params_cookiecutter["output_path"]
                    , self.params_cookiecutter["repo"]
                )
        # Crear un entorno virtual en la carpeta del proyecto
        venv_dir = os.path.join(project_dir, "venv")
        if not os.path.exists(venv_dir):
            subprocess.run(["python", "-m", "venv", venv_dir], check=True)
    
    def _install_lib(self):
        # Instalar las librerías necesarias en el entorno virtual y activarlo
        project_dir = os.path.join(
                    self.params_cookiecutter["output_path"]
                    , self.params_cookiecutter["repo"]
                )
        venv_dir = os.path.join(project_dir, "venv", "Scripts","python.exe")
        
        subprocess.call([venv_dir, "-m", "pip", "install", "-U", "pip"])
        subprocess.call([venv_dir, "-m", "pip", "install", "-e", project_dir])


    def _clear_console(self, step):
        """
        Funcion encargada de imprimir el header
        y mantener limpia la consola cuando avance
        el usuario entre las diferentes opciones
        ---------
        Parameters
        ---------
        first_time : bool, opcional, default=False
        Indica si es el primer header para indicar
        el saludo de bienvenida
        """
        os.system('cls')
        print("+---------------------------------------------------------------------------+")
        print("|                    GENERADOR DE LA BASE CALENDARIZABLE                    |")
        print("+---------------------------------------------------------------------------+")
        print("| Para mayor información de la herramienta consulte aquí: https://t.ly/xmW_f |")
        print("+---------------------------------------------------------------------------+")
        if step == 1:
            print("|       Te damos la bienvenida al generador de la base calendarizable       |")
            print("|        Le recomendamos leer cada nota dispuesta en el paso a paso         |")
            print("|              para que pueda generar su base exitosamente.              |")
            print("+---------------------------------------------------------------------------+")

    def generate_template(self): 
        """
        Funcion encargada de ejecutar
        la generacion del template
        haciendo uso de la libreria
        cookiecutter
        """
        self._default_params()
        self._step_manager()
        try:
            cookiecutter(
                template=os.path.join(
                    self._get_templates_path()
                    , self._template
                )
                , extra_context=self.params_cookiecutter
                , no_input=True
                , output_dir=self.params_cookiecutter["output_path"]
            )

            if self.user_venv:
                self._create_virtualenv()
                if self.user_lib:
                    self._install_lib()
            print("+-----------------------------------------+")
            print("|    EJECUCION FINALIZADA EXITOSAMENTE    |")
            print("+-----------------------------------------+")
            print("Gracias por utilizar el generador. Puedes encontrar tu paquete en la ruta: {}".format(
                os.path.join(
                    self.params_cookiecutter["output_path"]
                    , self.params_cookiecutter["repo"]
                )
            ))
        except BaseException as err:
            print("Error log ({}) generando la plantilla".format(
                err
            ))