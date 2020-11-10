# -*- coding: utf-8 -*-
"""
Created on Mon Nov  9 19:02:08 2020

@author: mthom
"""

import pandas as pd
from password import password
from sqlalchemy import create_engine

pw = password()
con = create_engine('mysql+mysqlconnector://{0}:{1}@{2}/mlb_db'.format(pw.user, pw.password, pw.host))

SQL = 'Select game_id, start, id, max(Home) Home, sum(PA) PA, sum(AB) AB, sum(SAC) SAC, sum(HBP) HBP, sum(BB) BB, sum(S) S, sum(D) D, sum(T) T, sum(HR) HR, sum(H) H, sum(TB) TB, sum(SO) SO, sum(RBI) RBI, sum(R) R, sum(CS) CS, sum(SB) SB from at_bats group by game_id, id Order By id Asc, start Asc'
df = pd.read_sql_query(SQL, con)

df['pts'] = 3*df['S'] + 6*df['D'] + 9*df['T'] + 12*df['HR']  +  3 * df['BB'] + 3 * df['HBP'] + 3.2 * df['R'] + 3.5 * df['RBI'] + 6 * df['SB']
df['number'] = df['game_id'].str[-1]
df['fanduel_points'] = df['pts']
df[df['fanduel_points'].notnull()][['game_id','start','number','id','fanduel_points']].to_sql(con=con, if_exists='append', index=False, name='batter_points', chunksize=10000)
con.dispose()    
