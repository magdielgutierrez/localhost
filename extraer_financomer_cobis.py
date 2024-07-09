import pandas as pd

dfs_banco=pd.read_csv('./P35/files/AT03_20231130.txt', sep='~'
                    ,dtype='str'
                    ,encoding = "ISO-8859-1"
                    ,keep_default_na=False
                    ,header=None
                    ,usecols=[13])

    
dfs_banco.rename(columns={13:'fn_cuenta'}, inplace=True)
dfs_banco=dfs_banco['fn_cuenta']

listaFinan=[]

for item in dfs_banco.values.tolist():
    if  item.startswith('0610'):
        listaFinan.append(item[4:])
print(len(listaFinan))

row='605720'
if row in listaFinan:
    print(row + '**')