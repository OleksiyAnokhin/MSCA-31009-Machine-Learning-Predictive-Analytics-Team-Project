# -*- coding: utf-8 -*-
"""
Created on Mon Nov 23 11:37:24 2020

@author: mthom
"""


import pandas as pd
from sqlalchemy import create_engine
from password import password

pw = password()
con = create_engine('mysql+mysqlconnector://{0}:{1}@localhost/mlb_db'.format(pw.user, pw.password))
SQL = 'Select id, date, team, opp, number, points as pred from rotogrinders where Position != "P"'
df = pd.read_sql_query(SQL, con)


SQL = 'Select `start`, number, id, fanduel_points as actual from batter_points where year(start)>=2017'
pts = pd.read_sql_query(SQL, con)


pts['date'] = pts['start'].apply(lambda x: x.date())
df = df.merge(pts[['id','date','number','actual']], how='left', on=['id','date','number'], validate='m:1')

df['yr'] = df['date'].apply(lambda x: x.year)

cor = df[['actual','pred','yr']].groupby('yr').corr().reset_index()
cor = cor[cor['level_1']=='actual'][['yr','pred']]
cor.set_index('yr', inplace=True)
cor.columns = ['correlation']

cor.loc['Total','correlation'] =  df[['actual','pred']].corr().iloc[0,1]


df['residuals'] = df['actual'] - df['pred']
df['MAE'] = df['residuals'].abs()
df['RMSE'] = df['residuals']**2

err = df[['yr','MAE','RMSE']].groupby('yr').mean()
err['RMSE'] = err['RMSE']**0.5
err.loc['Total', 'MAE'] = df['residuals'].abs().mean()

err.loc['Total', 'RMSE'] = df['RMSE'].mean()**0.5
cor = cor.join(err)


print(cor)
