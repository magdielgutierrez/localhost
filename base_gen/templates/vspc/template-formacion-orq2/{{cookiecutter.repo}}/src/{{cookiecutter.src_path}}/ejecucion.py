"""
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-- {{cookiecutter.vp_max}}
-----------------------------------------------------------------------------
-- Fecha Creación: {{cookiecutter.creation_date}}
-- Última Fecha Modificación: {{cookiecutter.creation_date}}
-- Autores: {{cookiecutter.package_author}}
-- Últimos Autores: {{cookiecutter.package_author}}
-- Descripción: Script de ejecución de la rutina
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
"""
import os
import warnings
import pkg_resources
import pandas as pd
from datetime import datetime

from orquestador2.orquestador2 import Orchestrator
from master_validation import Master_val
from vspc_respaldo_logs import Respaldo_logs
from vspc_respaldo_logs import SaveLogFile

from {{cookiecutter.src_path}}.etl import Etl
from {{cookiecutter.src_path}}.utils import leer_argumentos_command_line, establecer_logs

warnings.simplefilter(action='ignore', category=UserWarning)

def ejecutar(args):
    """
    Función que define la secuencia lógica requerida para
    la ejecución de los componentes que integran
    la base calendarizable
    """
    # Fecha/hora inicio ejecución
    start = datetime.now()
    warnings.resetwarnings()

    #Manejo de rutas
    ruta = pkg_resources.resource_filename(__name__, 'static/')
    ruta_ctrl = str("{}/{}".format(os.path.abspath(ruta),"xlsx/{{cookiecutter.excel}}.xlsm"))
    df = pd.read_excel(ruta_ctrl,index_col=False)
    excepcion = ""
    try:
        #Instancia del orquestador
        nom_proyecto = "{{cookiecutter.package_name}}"
        kw = establecer_logs(args.lcomp,args.lest,ruta)
        steps = [Etl(**kw)]
        orquestador = Orchestrator(nom_proyecto, steps, **kw)


        #Instancia del maestro de validaciones. El argumento de activar es para activar o desactivar la validación
        mv = Master_val(
            df_input=df,
            zona_tabla_output="{{cookiecutter.zona_master}}.val_{{cookiecutter.excel}}",
            sparky=orquestador.sparky,
            activar=False
        )

        #Si la validación es activa, se ejecuta la validación de insumos
        if mv.activar:
            if mv.insumo():
                orquestador.ejecutar()
            else:
                orquestador.sparky.logger.error("Insumos con mala calidad")
        else:
        #Si la validación es inactiva, se ejecuta el proceso sin validación
            orquestador.ejecutar()
    except Exception as e:
        #Se captura la excepción y se guarda en el log
        excepcion = e
        orquestador.sparky.logger.error(msg = e)
        raise e
    finally:
        #Se guarda el log en el archivo de logs y se sube a la LZ con el Respaldo de Logs
        slf = SaveLogFile(active=True)
        Respaldo_logs(sparky=orquestador.sparky,ruta=ruta)
        slf.save_log_file(orquestador, start, excepcion=excepcion)

if __name__ == '__main__':
    """
    Condición que permite obtener parámetros y ejecutar
    la lógica del presente archivo(archivo principal)
    """
    args = leer_argumentos_command_line()
    ejecutar(args)