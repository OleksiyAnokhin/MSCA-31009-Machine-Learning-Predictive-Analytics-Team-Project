# -*- coding: utf-8 -*-
"""
Created on Sat Jan  4 08:34:46 2020

@author: -
"""


import pandas as pd
import mysql.connector
from password import password

pd.options.display.max_columns=30

pw = password()
db = mysql.connector.connect(host=pw.host, user=pw.user, password=pw.password, database='mlb_db')

#### Grouped by Home Team #####
SQL = 'SELECT count(id) ct, (SUM(S) + 2*SUM(D) + 3*SUM(T) + 4*SUM(HR)) / SUM(AB) AS SLG, (SUM(H) + SUM(BB) + SUM(HBP)) / SUM(PA) AS OBP FROM at_bats where Home = 0'
total = pd.read_sql_query(SQL, db)
total.dropna(inplace=True)
total = total[total['ct']>50]
total['OPS'] = total['SLG'] + total['OBP']

SQL = 'SELECT count(id) ct, opp, (SUM(S) + 2*SUM(D) + 3*SUM(T) + 4*SUM(HR)) / SUM(AB) AS SLG, (SUM(H) + SUM(BB) + SUM(HBP)) / SUM(PA) AS OBP FROM at_bats WHERE Home = 0 GROUP BY opp'
stadium = pd.read_sql_query(SQL, db)
stadium.dropna(inplace=True)
stadium['OPS'] = stadium['SLG'] + stadium['OBP']

stadium['park_factor_SLG'] = stadium['SLG'] / total.loc[0, 'SLG']
stadium['park_factor_OBP'] = stadium['OBP'] / total.loc[0, 'OBP']
stadium['park_factor_OPS'] = stadium['OPS'] / total.loc[0, 'OPS']
stadium.rename(columns={'opp':'park'},inplace=True)

stadium.sort_values('park_factor_OPS', ascending=False, inplace=True)
stadium.reset_index(inplace=True, drop=True)

stadium.to_pickle('./park_factors.pkl')
