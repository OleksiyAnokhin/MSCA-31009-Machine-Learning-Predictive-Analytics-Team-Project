# -*- coding: utf-8 -*-
"""
Created on Mon Nov  9 19:00:04 2020

@author: mthom
"""


import pandas as pd
from sqlalchemy import create_engine
from password import password
   
pd.options.display.max_columns = 50

pw = password()
engine = create_engine('mysql+mysqlconnector://{0}:{1}@{2}:3306/mlb_db'.format(pw.user, pw.password, pw.host))

cols = [0, 1, 3, 6, 9, 10, 101, 103, 105, 107, 108, 110, 111, 113, 114, 116, 117, 119, 120, 122, 123, 
            125, 126, 128, 129, 131, 132, 134, 135, 137, 138, 140, 141, 143, 144, 146, 147, 149, 150, 152, 
            153, 155, 156, 158]
    
col_names = ['date', 'number', 'team0','team1','score0','score1', 'pitcher0','pitcher1','away1', 
                 'awayPos1','away2','awayPos2','away3','awayPos3','away4','awayPos4','away5','awayPos5',
                 'away6','awayPos6','away7','awayPos7','away8','awayPos8','away9','awayPos9', 'home1',
                 'homePos1','home2','homePos2','home3','homePos3','home4','homePos4','home5','homePos5'
                 ,'home6','homePos6','home7','homePos7','home8','homePos8','home9','homePos9']

for yr in range(2006, 2020):
    
    df = pd.read_csv('.//data//{0}//gl{0}.zip'.format(yr), header=None, usecols=cols, names=col_names)
    
    for col in ['date', 'score0', 'score1']:
        df[col] = df[col].apply(int)
    
    def dateInfo(df):
        year = df['date']//10000
        month = df['date']%10000//100
        day = df['date']%100
        return year, month, day
    
    df[['year','month','day']] = df.apply(dateInfo, axis=1, result_type='expand')
    df['game_id'] = df['date'].astype(str) + df['number'].astype(str) + df['team1']
    
    
    df.to_sql(con=engine, name='games', if_exists='append', index=False)    
    
    print(yr)
       