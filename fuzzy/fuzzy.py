from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import pandas as pd
from datetime import datetime
import time
import re



inicio= time.time()

df=pd.read_csv('./file/vplus_export_id_banco.txt', sep='|')

df_ente=pd.read_csv('./file/vplus_entes.txt', sep='|')


def fuzzy_fuzzy(part_name,full_name):
    tmp=df_ente[df_ente['en_nomlar'].str.contains(part_name,case=False)].reset_index(drop=True)
    print(f"\t|-->El nombre se buscara en {tmp.shape[0]} filas")
    
    try:
        for index, row in tmp.iterrows():
            
            tmp.loc[index,'ratio']=fuzz.ratio(full_name, row['en_nomlar'])
            
        tmp=tmp.sort_values(by='ratio', ascending=False)
        
        return '*'.join(tmp.iloc[[0]].values.flatten().astype(str))
    except:
        return '0*0*0*0'

    
#df=df.sample(10)

for index, row in df.iterrows():
    ahora=datetime.now()
    print(f"{index}  - {row['nom_cliente']} {ahora.hour}:{ahora.minute}:{ahora.second}")
    name=max(row['nom_cliente'].split(),key=len)
    name=re.sub(r'[^a-zA-Z\s]','',name)
    ratio=fuzzy_fuzzy(name,row['nom_cliente'])
    df.loc[index,'full']=ratio
    print()
    
    #break


df[["banco_id", "name", "ruc", "porc_macth"]] = df.full.str.split("*", expand=True)

final=time.time()
print(f'Tiempo transcurrido: {round(final-inicio)} segundos.')
print("sample df output")
print(df)
df.to_excel('export.xlsx')



