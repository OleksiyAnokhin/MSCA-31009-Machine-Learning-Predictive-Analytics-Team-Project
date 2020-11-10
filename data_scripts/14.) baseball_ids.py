# -*- coding: utf-8 -*-
"""
Created on Sun Nov  8 13:13:47 2020

@author: mthom
"""

import pandas as pd
from sqlalchemy import create_engine
from password import password

df = pd.read_csv('http://crunchtimebaseball.com/master.csv', encoding='latin-1')

df = df[df['retro_id'].notna()]    
df['names'] = df['mlb_name'].str.split()


#Split names into first and last name since Box score files only have last name, first initial

#remove Jr and Sr from names
df['names'] = df['names'].apply(lambda x: x[:-1] if (len(x)>=3) and ('iii' in x[-1].lower() or 'jr' in x[-1].lower() or 'sr' in x[-1].lower()) else x)
df['name_length'] = df['names'].apply(lambda x: len(x))

#combine first and second values for players with a space in first name
df['names'] = df.apply(lambda df: [df['names'][0]+' '+df['names'][1]]+[df['names'][2]] if (len(df['names'])==3)&(df['names'][-1][0:4].lower()+df['names'][0][0].lower() == df['retro_id'][0:5]) else df['names'], axis=1)
df['name_length'] = df['names'].apply(lambda x: len(x))

#combine last two values for players with a space in last name
df['names'] = df.apply(lambda df:[df['names'][0], df['names'][1] + ' ' + df['names'][2]] if (len(df['names'])==3)&(str(df['names'][1].lower() + df['names'][-1].lower())[0:4] == df['retro_id'][0:4]) else df['names'], axis=1)
df['name_length'] = df['names'].apply(lambda x: len(x))

#players with 3 parts to last name
df['names'] = df.apply(lambda df: [df['names'][0], df['names'][1]+ ' ' +df['names'][2]+ ' ' +df['names'][3]] if len(df['names'])==4 and str(str(df['names'][1]+df['names'][2]+df['names'][3])[0:4]+df['names'][0][0])[0:5].lower() == df['retro_id'][0:5] else df['names'], axis=1)
df['name_length'] = df['names'].apply(lambda x: len(x))

#players with 3 part names and  last name less than 4 characters
df['names'] = df.apply(lambda df: [df['names'][0]+' '+df['names'][1], df['names'][2]] if (df['names'][-1].lower()+'-' == df['retro_id'][0:4]) & (len(df['names'])==3) else df['names'], axis=1)
df['name_length'] = df['names'].apply(lambda x: len(x))

#check that all are length 2
if sum(df['name_length']!=2):
    raise Exception('names not properly parsed')

#populate first and last names
df['fname'] = df['names'].apply(lambda x: x[0])
df['lname'] = df['names'].apply(lambda x: x[1])
df.drop(['names','name_length'], axis=1, inplace=True)

#upload to SQL
pw = password()
con = create_engine('mysql+mysqlconnector://{0}:{1}@{2}/mlb_db'.format(pw.user, pw.password, pw.host))
df.to_sql(con=con, name='id', if_exists='append', index=False)

