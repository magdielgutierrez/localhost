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
from vspc_config_utils import Parametrizador

from {{cookiecutter.src_path}}.preprocesador import Preprocesador
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
    ruta_central = str("{}/{}".format(os.path.abspath(ruta),"xlsx/central_parametros.xlsx"))
    excepcion = ""
    try:
        nom_proyecto = "{{cookiecutter.package_name}}"
        kw = establecer_logs(args.lcomp,args.lest,ruta)
        steps = [ Preprocesador(**kw)]
        orquestador = Orchestrator(nom_proyecto, steps, **kw)

        parametrizador = Parametrizador(indice="{{cookiecutter.index}}",
                       zona='proceso_serv_para_los_clientes',
                       ruta = ruta_central,
                       sparky = orquestador.sparky,
                       zona_p = 'proceso')

        if args.p:
            parametrizador.subir_tabla_central()

        ### GENERACION DE TABLAS PARAMETRICAS

        parametrizador.obtener_tablas_parametricas()

        ### CALIDAD E INGESTIONES

        ## Instanciamiento del maestro, apuntando a la tabla parametrica creada
        mv = Master_val(
            zona_tabla_input = "proceso.{{cookiecutter.index}}_masterval_ctrls",
            zona_tabla_output="proceso.{{cookiecutter.index}}_masterval_ctrls_val_hist",
            sparky=orquestador.sparky
            )
        ## Ejecución del pilar de insumos, para validar la calidad de datos en insumos centrales
        if mv.insumo():

            ## Ejecución del pilar de ejecución, el cual valida que el orquestador se ejecute correctamente
            # Dicho orquestador, ejecuta la logica del preprocesador, que consulta las ultimas ingestiones de los insumos centrales
            if mv.ejecucion(orquestador_dos = orquestador):
                orquestador.sparky.logger.info(msg = 'Ejecución correcta')
            else:
                orquestador.sparky.logger.error("Ejecución incorrecta")
        else:
            orquestador.sparky.logger.error("Insumos con mala calidad")

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