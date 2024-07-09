# -*- coding: utf-8 -*-

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
from vspc_config_utils.config_utils import Config_utils
from vspc_respaldo_logs.respaldo_logs import Respaldo_logs
from vspc_respaldo_logs import SaveLogFile
from {{cookiecutter.src_path}}.etl import Etl
from {{cookiecutter.src_path}}.preprocesador import Preprocesador
from orquestador2.orquestador2 import Orchestrator
import os
import sys
import pkg_resources
from master_validation.master_val import Master_val
import pandas as pd
from datetime import datetime
import json
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

def ejecutar(params=""):
    """
    Función que define la secuencia lógica requerida para
    la ejecución de los componentes que integran
    la base calendarizable
    """
    # Fecha/hora inicio ejecución
    start = datetime.now()
    ruta = pkg_resources.resource_filename(__name__, 'static/')
    path = pkg_resources.resource_filename(__name__, "")
    logs_path = os.path.join(path.split("src")[0], "logs_calendarizacion")
    ruta_ctrl = str("{}/{}".format(os.path.abspath(ruta),"xlsx/{{cookiecutter.excel}}.xlsm"))
    df_input = pd.read_excel(ruta_ctrl,index_col=False)
    warnings.resetwarnings()
    orq_error = False
    try:
        if not os.path.exists("logs/"):
            os.mkdir("logs/")
        if not os.path.exists(logs_path):
            print("No existía la carpeta de logs para calendarización, se está creando.")
            os.mkdir(logs_path)
        nom_proyecto = "{{cookiecutter.package_name}}"
        steps = [ Preprocesador(), Etl() ]
        orquestador = Orchestrator(nom_proyecto, steps)
        mv = Master_val(
                #zona_tabla_input="{{cookiecutter.zona_master}}.ctrl_{{cookiecutter.excel}}",
                df_input=df_input,
                zona_tabla_output="{{cookiecutter.zona_master}}.val_{{cookiecutter.excel}}",
                insumos=False,
                ejecucion=False,
                razonabilidad=False,
                sparky=orquestador.sparky
                )
        if subir_xlsx:
            c_util = Config_utils(sparky=orquestador.sparky)
            c_util.load_xlsx_lz(ruta=ruta)
        elif params=="--l":
            kw = {}
            steps = [ Etl(**kw) ]
            orquestador = Orchestrator(nom_proyecto, steps, **kw)
            orquestador.ejecutar()
        elif params=="" and mv.activar:
            if mv.insumo():
                if mv.ejecucion(orquestador):
                    mv.razonabilidad()
                else:
                    print("Se genero un error en la ejecución")
            else:
                print("Los insumos presentan errores que afectan la ejecución del flujo")
        else:
            orquestador.ejecutar()
    except Exception as e:
        orq_error = e
    finally:
        # Instanciar el respaldo de logs
        slf = SaveLogFile(active=True)
        Respaldo_logs(sparky=orquestador.sparky,ruta=ruta)
        if orq_error:
            slf.save_log_file(orquestador, start, orq_error)
            orquestador.sparky.logger.error(msg = orq_error)
            raise orq_error
        else:
            slf.save_log_file(orquestador, start)

def actualizar_fechas_ejecucion(fecha):
    ruta_json=pkg_resources.resource_filename(__name__, 'static/config.json')
    year = fecha.year
    month = fecha.month
    day = fecha.day

    with open(ruta_json, 'r') as file:
        data = json.load(file)
        data['global']['parametros_lz']['fecha_num']= int(fecha.strftime('%Y%m%d'))
        data['global']['parametros_lz']['year']= year
        data['global']['parametros_lz']['month']= month
        data['global']['parametros_lz']['day']= day

    with open(ruta_json, 'w') as file:
        json.dump(data, file, indent=4)

def leer_argumentos_command_line():

    # Definimos default values para los parametros
    subir_xlsx = False
    fecha=datetime.now().date() #numero dias antes

    # Loop leer argumentos pasados command-line
    for arg in sys.argv[1:]:
        if arg=="--p" or arg=="--subir-tabla":
            subir_xlsx = True
        elif arg.startswith("--f=") or arg.startswith("--fecha="):
            fecha = arg.split("=")[1]
            try:
                fecha=datetime.strptime(fecha,'%Y%m%d').date()
            except:
                raise Exception("fecha no valida, recuerde ingresarla en formato yyyymmdd")

    return subir_xlsx, fecha

if __name__ == '__main__':
    """
    Condición que permite obtener parámetros y ejecutar
    la lógica del presente archivo(archivo principal)
    """
    subir_xlsx, fecha = leer_argumentos_command_line()
    actualizar_fechas_ejecucion(fecha)
    ejecutar(subir_xlsx)