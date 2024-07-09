import os
import json
import argparse
import pkg_resources
from datetime import datetime
from vspc_config_utils.config_utils import Config_utils


def leer_argumentos_command_line():
   
    parser = argparse.ArgumentParser()

    parser.add_argument("--p", "--subir-tabla", action='store_true', help="Subir tabla xlsx")
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