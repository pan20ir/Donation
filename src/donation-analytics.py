
from itertools import islice
import pandas as pd
import numpy as np
import os
#################   READ PERCENTILE.TXT ##################################
percentile_file = open('percentile.txt', 'r')
perc_str = percentile_file.readline()
percentile = int(perc_str)
#################   READ ITCONT.TXT  ##################################
f = open('itcont.txt', 'r')
size = os.stat('itcont.txt').st_size
ff = islice(f,size)
header  = ['CMTE_ID','NAME','ZIP_CODE', 'TRANSACTION_DT','TRANSACTION_AMT','OTHER_ID']
selected =[]
for line in ff:
    ln_split = line.split('|')
    try:
     lst = [ln_split[0],ln_split[7:8],ln_split[10][:5],ln_split[13],int(ln_split[14]),ln_split[15]]
     selected.append(lst)
    except:
        pass
############################  Functions for cleaning data ##########################      
def correctzip(item):
    import re
    r = re.compile('\d{5}')
    if item.isdigit() == True and len(item) > 3:
      if r.match(item):  
         return True
      else:
        return False
    return False

def correctdate(item):
   if item.isdigit() == True:
        return True
   else:
        return False
#################################  Creating DataFrame and Filetring ##############################################
dat = pd.DataFrame(data = selected,index=None,columns = header)
dat['NAME'] = dat['NAME'].apply(lambda x:x[0])
final_data = dat[dat['OTHER_ID'] == ''].replace('','empty')
final_data['correct_zip'] = final_data['ZIP_CODE'].apply(lambda x: correctzip(x))
final_data['correct_date'] = final_data['TRANSACTION_DT'].apply(lambda x: correctdate(x))
final_data = final_data[(final_data['correct_zip'] == True)&(final_data['correct_date'] == True)]
final_data = final_data[(final_data['TRANSACTION_DT'] != empty)&(final_data['TRANSACTION_AMT'] > 0)]
final_data['ZIP_CODE'] = final_data['ZIP_CODE'].apply(lambda x: int(x))
final_data = final_data[final_data['ZIP_CODE'] > 612]
final_data['TRANSACTION_DT'] = final_data['TRANSACTION_DT'].apply(lambda x: int(str(x)[-4:]))
final_data['Duplicates'] = final_data['NAME'].duplicated()
final_data = final_data[final_data['Duplicates'] == True]
del final_data['correct_zip']
del final_data['correct_date']
del final_data['Duplicates']
del final_data['OTHER_ID']
################################# Preparing data in the correct format ###############################################
################################# This step is timetaking and slowest #############################################
col = ['CMTE_ID','NAME','ZIP_CODE','TRANSACTION_DT','PERCENTILE','CUMSUM','COUNT']
fin = pd.DataFrame(columns = col)
grouped = final_data.groupby(['NAME','ZIP_CODE'],sort=False)
for item in grouped:
  data = item[1:2][0].reset_index()
  counter = pd.Series(arange(1,len(data)+1),index=None)
  data['PERCENTILE'] = round(np.percentile(data['TRANSACTION_AMT'],percentile,interpolation='nearest'),1)
  data['CUMSUM'] = data['TRANSACTION_AMT'].cumsum()
  data['COUNT'] = counter
  fin = pd.concat([fin,data],ignore_index=True)
fin = fin[['CMTE_ID','ZIP_CODE','TRANSACTION_DT','PERCENTILE','CUMSUM','COUNT']]
fin.to_csv('repeat_donors.txt', header=False, index=False, sep='|', mode='a')
#+++++++++++++++ End of Script +++++++++++++++++++++++++++++++++++++++++++++++++
