# {{cookiecutter.package_title}}

{{cookiecutter.package_description}}

---

[[_TOC_]]

---

## Introducción

El objetivo de este proyecto es tener una base de desarrollo analitico para su posterior puesta en produccion.

## Pasos para la Instalación y Ejecución del Repo

Es necesario para la implementacion de este Repo contar con los siguientes permisos :

* Usuario SAS/LZ
* Permisos VPN ([Solicitud USM](https://usm/usm/wpf?Node=icguinode.catalogitemdetails&Args=12021&ObjectID=12021&NspPath=&searchIn=&searchInstr=)) , Formato A ([Ver Comunidad JOIN](https://bancolombia.sharepoint.com/teams/ComunidadJoin525/WikiJOIN/Inicio.aspx))
* Debe contar con aceso a la Fuentes ([Solicitud USM](https://usm/usm/wpf?Node=icguinode.catalogitemdetails&Args=11291&ObjectID=11291&NspPath=&searchIn=&searchInstr=))
* Debe contar con create external table ([Solicitud USM](https://usm/usm/wpf?Node=icguinode.catalogitemdetails&Args=11291&ObjectID=11291&NspPath=&searchIn=&searchInstr=))
* Software Git Bash ([Solicitud USM](https://usm/usm/wpf?Node=icguinode.catalogitemdetails&Args=11036&ObjectID=11036&NspPath=&searchIn=&searchInstr=))
* Tener Instalado ODBC_Impala ([Solicitud USM](https://usm/usm/wpf?Node=icguinode.catalogitemdetails&Args=10942&ObjectID=10942&NspPath=&searchIn=&searchInstr=))


#### Instalación (Calendarizable / No calendarizable):

* 1. Crear ambiente virtual dentro de la carpeta del proyecto

  > pip install virtualenv (si no lo tiene instalado)
  > virtualenv venv
  >
* 2. Luego debe activar el entorno virtual

  > .\venv\Scripts\activate
  >
* 3. Instalar librerias (Calendarizable )

  > pip install {{cookiecutter.package_name}}
  >



---
## Ejecución

Si se trabaja con un ambiente virtual, se debe activar primero. [**Más información sobre el uso de ambientes virtuales**](https://dev.azure.com/GrupoBancolombia/Vicepresidencia%20de%20Innovaci%C3%B3n%20y%20Transformaci%C3%B3n%20Digital/_wiki/wikis/Vicepresidencia-de-Innovaci%C3%B3n-y-Transformaci%C3%B3n-Digital.wiki/20351/Sobre-el-uso-de-ambientes-virtuales-Python-3.5).

*Se debe ejecutar el siguiente comando:*
```
python -m {{cookiecutter.src_path}}.ejecucion
```

Para efectos de la generación de logs para la calendarización se pueden indicar los parámetros directamente en los siguientes comandos.

*Logs de estabilidad:*
```
python -m {{cookiecutter.src_path}}.ejecucion -lt "est"
```

*Logs de compilación:*
```
python -m {{cookiecutter.src_path}}.ejecucion -lt "cmp" -pl [porcentaje]
```

En estos comandos para la calendarización el parámetro ```lt``` hace referencia al tipo de log, estabilidad ```est``` o compilación ```cmp```. Cabe resaltar que, si se va a generar un log de compilación, se requiere también el parámetro ```pl``` que hace referencia al porcentaje límite de datos que se toma de las tablas insumo para dicha ejecución (valor entero entre 1 y 100). De igual manera, si se habilita tanto el log de estabilidad o el de compilación, la carpeta para almacenar los logs generados será ```logs_calendarizacion``` y para otros casos la carpeta será ```logs```; en ambos casos se creará la carpeta en el directorio de trabajo actual.

Adicionalmente, con el fin de abreviar los comandos de ejecución, se habilitó utilizar el nombre del paquete (con guiones bajos) directamente para reemplazar la sintáxis de módulos de python; lo cuál permite sustituir la expresión ```python -m {{cookiecutter.src_path}}.ejecucion``` por ```{{cookiecutter.src_path}}``` en cada comando de ejecución si así lo desea.

---
## Prerrequisitos

El paquete ha sido generado para la versión de Python
	```
    {{cookiecutter.python_version}}
    ```
. Las librerías o paquetes necesarios para la ejecución son:
- `orquestador2>=1.2.2`
- `vspc-config-utils`
- `vspc_respaldo_logs`

---
## Insumos y resultados

Los insumos utilizados en el proceso son:

| Zona de Datos | Tabla |
| - | - |
| _zone_x_ | _table_y_ |
| _zone_z_ | _table_k_ |

Los resultados obtenidos son:

| Zona de Datos | Tabla | Descripción | Tipo de Ingestión |
| - | - | - | - |
| _zone_results_ | _table_n_ | Esta información debe describir la tabla. | Incremental |
| _zone_results_ | _table_h_ | Esta información debe describir la tabla. | Full |

---