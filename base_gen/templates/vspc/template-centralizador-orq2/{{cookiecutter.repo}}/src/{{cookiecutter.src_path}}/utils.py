import os
import json
import argparse
import pkg_resources
from datetime import datetime
from vspc_config_utils.config_utils import Config_utils


def actualizar_fechas_ejecucion(fecha):
    ruta_json=pkg_resources.resource_filename(__name__, 'static/config.json')
    with open(ruta_json, 'r+') as file:
        data = json.load(file)

        data['global']['parametros_lz']['fecha_num'] = int(fecha.strftime('%Y%m%d'))
        data['global']['parametros_lz']['year'] = fecha.year
        data['global']['parametros_lz']['month'] = fecha.month
        data['global']['parametros_lz']['day'] = fecha.day

        file.seek(0)

        json.dump(data, file, indent=4)

        file.truncate()


def leer_argumentos_command_line():

    parser = argparse.ArgumentParser()

    parser.add_argument("--p", "--parametrizador", action='store_true', help="Subir parametricas xlsx")
    parser.add_argument("--f", "--fecha", type=lambda s: datetime.strptime(s, '%Y%m%d').date(), default=datetime.now().date(), help="Fecha en formato yyyymmdd")
    parser.add_argument("--lcomp", action='store_true', help="Logs de compilacion")
    parser.add_argument("--lest", action='store_true', help="Logs de estabilizacion")

    args = parser.parse_args()

    return args


def establecer_logs(lcomp,lest, path):

    with open(f'{path}/config.json') as f:
        config = json.load(f)

    logs_path = os.path.join(path.split("src")[0], "logs_calendarizacion")
    if not os.path.exists(logs_path):
            os.mkdir(logs_path)

    kw = {}
    if lcomp:
        kw.update(config['global']['logs_calendarizacion']['compilacion'])
        kw["log_path"] = logs_path
    elif lest:
        kw.update(config['global']['logs_calendarizacion']['estabilidad'])
        kw["log_path"] = logs_path

    return kw

def subir_df(sp, df, tabla, zona_r):
    c_util = Config_utils(sparky=sp)
    c_util.load_df_lz(df=df,tabla = tabla, zona_r=zona_r)
    return True