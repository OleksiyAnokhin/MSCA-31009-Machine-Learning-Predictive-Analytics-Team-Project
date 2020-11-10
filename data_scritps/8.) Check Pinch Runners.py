# -*- coding: utf-8 -*-
"""
Created on Sat Mar  3 15:00:46 2018

@author: Mike Thompson
"""

import zipfile as zp
import pandas as pd
from csv import reader

#verify that unmatched records actually are pinch runners
for year in range(2006, 2020):
    Raw_Data = []
    path = '.\\data\\{}\\'.format(year)
    filepath = path + str(year) + 'eve.zip'
    zfile = zp.ZipFile(filepath, mode='r')
    file_list = [a for a in zfile.namelist() if a[-3:-1]=='EV']
    
    for team_file in file_list:
        text = zfile.read(team_file)
        text = str(text)
        text = text.split(sep=r'\n')
    
        ct = 0
        for b in reader(text):
            ct+=1
            
            b= [x.replace(r'\r', '') for x in b]
            
            if b[0] == 'id' or b[0]=='b\'id':
                game_id = b[1]
            
            if b[0] == 'play':
                inning = b[1]
                
            if b[0] == 'sub':
                if b[5] == '12' or b[5] ==12:
                    b.extend([game_id,inning])
                    Raw_Data.extend([b])

    df = pd.DataFrame(Raw_Data, columns=['sub','id','name','Home','batting order','position','game_id','inning'])
    df2 = pd.read_pickle(path + str(year) + '_events_with_sb.pkl')
    df2 = df2[(df2['event']=='Pinch Runner Scores')|(df2['event']=='Pinch Runner Caught Stealing')|(df2['event']=='Pinch Runner Steals Base')]
    
    for col in ['Home','inning']:
        df[col] = df[col].apply(int)
    
    df['flag'] = 1
    lenCheck = len(df2)
    df2 = df2.merge(df[['id', 'Home', 'game_id', 'inning','flag']], how='left', on=['id', 'Home', 'game_id', 'inning'])
    if lenCheck!=len(df2):
        raise Exception('merge error')
        
    print(year, len(df2), df2['flag'].sum(), len(df2) - df2['flag'].sum())


