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
from {{cookiecutter.src_path}}.preprocesador import Preprocesador
from {{cookiecutter.src_path}}.modelo import Modelo
from {{cookiecutter.src_path}}.utils import actualizar_fechas_ejecucion, leer_argumentos_command_line, establecer_logs

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
    ruta = pkg_resources.resource_filename(__name__, 'static/')
    ruta_ctrl = str("{}/{}".format(os.path.abspath(ruta),"xlsx/{{cookiecutter.excel}}.xlsm"))
    df = pd.read_excel(ruta_ctrl,index_col=False)
    excepcion = ""
    try:
        nom_proyecto = "{{cookiecutter.package_name}}"
        kw = establecer_logs(args.lcomp,args.lest,ruta)
        steps = [ Preprocesador(**kw), Etl(**kw), Modelo(**kw)]
        orquestador = Orchestrator(nom_proyecto, steps, **kw)

        mv = Master_val(
            df_input=df,
            zona_tabla_output="{{cookiecutter.zona_master}}.val_{{cookiecutter.excel}}",
            sparky=orquestador.sparky,
            activar=True
        )

        if mv.activar:
            if mv.insumo():
                orquestador.ejecutar()
            else:
                orquestador.sparky.logger.error("Insumos con mala calidad")
        else:
            orquestador.ejecutar()
    except Exception as e:
        excepcion = e
        orquestador.sparky.logger.error(msg = e)
        raise e
    finally:
        # Instanciar el respaldo de logs
        slf = SaveLogFile(active=True)
        Respaldo_logs(sparky=orquestador.sparky,ruta=ruta)
        slf.save_log_file(orquestador, start, excepcion=excepcion)

if __name__ == '__main__':
    """
    Condición que permite obtener parámetros y ejecutar
    la lógica del presente archivo(archivo principal)
    """
    args = leer_argumentos_command_line()
    actualizar_fechas_ejecucion(args.f)
    ejecutar(args)