import pandas as pd
import re
raw=pd.read_csv('./files/tdcx_ruc_ente_raw_001.txt',sep='|', dtype='str')

## PE000100493
def _updateFormatRuc(ruc):
    
    if '-' in ruc and 'E' in ruc:
        
        tmp_ruc=ruc.split('-')
        return ''.join(re.findall(r'[^(\d)]', tmp_ruc[0]))+'-' + str(int(tmp_ruc[1])) + '-'+ str(int(tmp_ruc[2]))

    elif '-' in ruc :
        
        tmp_ruc=ruc.split('-')
        try:
            return ''.join( str(int(tmp_ruc[0])) +'-' + str(int(tmp_ruc[1])) + '-'+ str(int(tmp_ruc[2])))
        except:
            return ruc

    elif 'N' in ruc :

        try:
             return ''.join(re.findall(r'[^(\d)]', ruc[0:5])) +'-' + str(int(ruc[5:9])) + '-'+ str(int(ruc[9:]))

        except:
            return ruc
        
           
    elif ruc[0:2]=='00':
        tpruc_1=ruc[2:]
        
        if tpruc_1[0:1]=='0':
            return tpruc_1[1:2] + '-' + tpruc_1[5:6]+ '-' + tpruc_1[-5:]
        
        elif tpruc_1[0:2]=='PE':
            return tpruc_1[0:2] + '-' + str(int(tpruc_1[2:6]))+ '-' +  str(int(tpruc_1[-5:]))
    elif ruc[0]=='0' and len(ruc)==13:
        try:
            return str(int(ruc[0:2])) +'-'+ str(int(ruc[2:8])) + '-'+ str(int(ruc[8:]))
        except:
            return ruc
    else:
        return ruc
    


raw['xruc']=raw.apply(lambda row:_updateFormatRuc(row['ruccliente'].strip()), axis=1) 
print(raw.sample(25))
raw.to_excel('./files/tdcx_ruc_cis.xlsx')