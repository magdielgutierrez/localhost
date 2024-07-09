# -*- coding: utf-8 -*-
"""

"""
from base_gen.generator import GeneratorManager
import sys
import pathlib

def main(ruta=False,nombre=False, tipo=False,indice=False, author = False, venv = False, lib = False):
    """
    ---------
    Funcion
    ---------
    Se encargada de ejecutar
    el generator
    ---------
    Parametros
    ---------
	Args:
		ruta (str, optional): El parámetro ruta indica donde se creara el repositorio. Ejemplo: "C:/Users/daijimen/Documents/"
		nombre (str, optional): El parámetro nombre indica el nombre del repositorio. Ejemplo: "vspc-gis-test".
		tipo (str, optional): El parámetro Tipo de plantilla. Opciones: (1. Migracion | 2. Etl | 3. Modelo).
        indice (str, optional): El parámetro índice indica el prefijo que será usado para el nombramiento de tablas. Ejemplo: "test".
        autor (str, optional): El parámetro autor indica el o los autores principales del repo, maximo 3. Ejemplo: "daijimen".
        venv (str, optional): El parámetro venv indica si el usuario quiere o no crear ambiente virtual. Recibe valores 1 o 0 para aceptar o negar la creación
        lib (str, optional): El parámetro lib indica si el usuario quiere o no instalar librerías base. Solo si creó el ambiente virtual. Recibe valores 1 o 0 para aceptar o negar la instalación
	"""
    gm = GeneratorManager(ruta,nombre, tipo,indice, author, venv, lib)
    gm.generate_template()
    
def _help():
    print('')
    print('Generador de la Base Calendarizable')
    print('python -m base_gen.new_package <comando>')
    print('Escriba el comando indicando los parametros de la librería, o ejecute sin comando para ser guiado paso a paso')
    print('python -m base_gen.new_package -n "nombre" -i "indice" -r "path/to/" -t "1|2|3" -v "0|1" -l "0|1"')
    print('Parámetros de la librería : ')
    print('  * -n "nombre"   El parámetro nombre indica el nombre del repositorio. Ejemplo: "vspc-gis-test"')
    print('  * -a "autor"    El parámetro autor indica el o los autores principales del repo, maximo 3. Ejemplo: "daijimen"')
    print('  * -i "indice"   El parámetro índice indica el prefijo que será usado para el nombramiento de tablas. Ejemplo: "test"')
    print('  * -r "path/to/" El parámetro ruta indica donde se creara el repositorio. Ejemplo: "C:/Users/daijimen/Documents/"') 
    print('  * -v "venv"     El parámetro venv indica si el usuario quiere o no crear ambiente virtual. Recibe valores 1 o 0 para aceptar o negar la creación')
    print('  * -l "lib"      El parámetro lib indica si el usuario quiere o no instalar librerías base. Solo si creó el ambiente virtual. Recibe valores 1 o 0 para aceptar o negar la instalación')
    print('  * -t "1|2|3|4"    El parámetro Tipo de plantilla. Opciones: ') 
    print('        1  Etl')
    print('        2  Modelo') 
    print('        3  Centralizador')
    print('        4  Formacion')

if __name__ == '__main__':
        try:
           nombre = False
           indice = False
           ruta = False
           tipo = False
           author = False
           venv = False
           lib = False
           option = True
           i = 1
           while i <= len(sys.argv):
                if len(sys.argv) > i and sys.argv[i] == "-n":
                   nombre = sys.argv[i+1]
                if len(sys.argv) > i and sys.argv[i] == "-i":
                   indice = sys.argv[i+1]
                if len(sys.argv) > i and sys.argv[i] == "-r":
                   ruta = sys.argv[i+1]
                if len(sys.argv) > i and sys.argv[i] == "-t":
                   tipo = sys.argv[i+1]
                if len(sys.argv) > i and sys.argv[i] == "-a":
                   author = sys.argv[i+1]
                if len(sys.argv) > i and sys.argv[i] == "-v":
                   venv = sys.argv[i+1]
                if len(sys.argv) > i and sys.argv[i] == "-l":
                   lib = sys.argv[i+1]
                if len(sys.argv) > i and sys.argv[i] == "--help":
                   option = False
                   break
                i += 1
           if option:
               main(ruta, nombre, tipo, indice, author, venv, lib)
           else:
               _help()
        except Exception as e:
            print(e) 