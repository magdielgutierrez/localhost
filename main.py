import pandas as pd

read_dic=pd.read_excel("./file/dic_kroner.xlsx",sheet_name='0089') 
rowLen=read_dic['colName'].to_list()   


file= open('./file/LPPAMI0KPN0089-Magdiel','r') 
rowFile=file.readlines()


rowData=[]


for row in rowFile:
    pivotDict={}
    iposx=0
    for posx in range(read_dic.shape[0]):
     
        colName=read_dic.iloc[posx][0].strip()
        fposx=iposx + (read_dic.iloc[posx][1])
        colValue=row[iposx:fposx]
        pivotDict[f'{colName}']=colValue
        
        iposx=fposx
        
    rowData.append(pivotDict)
   # break


# dfs_Final= pd.DataFrame.from_dict(pivotDict)

#print(rowData[2])

dfs_Final=pd.DataFrame.from_dict(rowData)
print(dfs_Final.sample(1).T)

#dfs_Final.sample(10).T.to_excel('output0089.xlsx')
     
