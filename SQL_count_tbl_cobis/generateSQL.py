import os, time
class GeneratorManager():
    def _menu():
        os.system('cls')
        print("+--------------------------------------------------------------------------+")
        print("|      GENERADOR DE SCRIPTS SQL PARA CONTEO/DECRIBE DE TABLAS COBIS        |")
        print("+--------------------------------------------------------------------------+")
    
    def _generate_counter(self,lst_tbls: list, module :str):
        
        linestr=[]
        for tbl in lst_tbls:
            linestr.append(f"SELECT '{module}' Nombre_Esquema, '{tbl}' Nombre_Tabla, Count(*) Conteo from cobis.{tbl}")
    
        querysql= " join \n".join(linestr)
        querysql = querysql + ";"
        
        
        return querysql
    
    def _generate_describe(self,lst_tbls: list, module :str):
        linestr=[]
        for tbl in lst_tbls:
            linestr.append(f"DESCRIBE {module}.{tbl};")
    
        querysql= "\n".join(linestr)
        return querysql
    
    def _savefilesql(self,textcontent:str):
        
        with open("output.sql", "w") as text_file:
            text_file.write(textcontent)

if __name__ == '__main__':
    lst_tbls=[]
    GeneratorManager._menu()
    print()
    module=str(input("    Ingrese el nombre del Módulo:"))
    tbl_int=int(input("    Número de tablas:"))
    print()
    print('   ----------')
    
    if tbl_int > 0:
        for tbl in range(1,tbl_int+1):
            lst_tbls.append(input(f"     Tabla No. {tbl} es: ").lower())
        
    print()
    _=input("|------>   ENTER PARA CONTINUAR....")
    os.system('cls')
    querycounter=GeneratorManager()._generate_counter(lst_tbls,module)
    querydescribe=GeneratorManager()._generate_describe(lst_tbls, module)
    
    textcontent= querycounter + "\n\n\n" + querydescribe
    
    print("Estamos trabajando en tus Queries....")
    time.sleep(1)
    GeneratorManager()._savefilesql(textcontent)  
    os.system('cls')
    print()
    print("+-----------------------------------------+")
    print("|    EJECUCION FINALIZADA EXITOSAMENTE    |")
    print("+-----------------------------------------+")
    print("File generado ----> output.sql")
          
          
