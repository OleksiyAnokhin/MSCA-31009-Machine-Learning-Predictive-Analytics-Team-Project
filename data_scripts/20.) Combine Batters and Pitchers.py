# -*- coding: utf-8 -*-
"""
Created on Sun Nov 24 13:08:59 2019

@author: mthom
"""

import pandas as pd
import datetime
from sqlalchemy import create_engine
from password import password
import numpy as np

pw = password()
con = create_engine('mysql+mysqlconnector://{0}:{1}@localhost/mlb_db'.format(pw.user, pw.password))

df = pd.read_pickle('./batters.pkl')
df2 = pd.read_pickle('./pitcher.pkl')

df2['year'] = df2['Date'].apply(lambda x: x.year)
#Switch perspective of team to batter's perspective
df2.rename(columns={'next_team':'next_opp','next_opp':'next_team','id':'p'}, inplace=True)

df2.drop(['date','game','team','home','opp','number','ER','W','L','QS', 'park_factor_SLG_avg','park_factor_OBP_avg',
         'park', 'next_park_factor_SLG', 'next_park_factor_OBP', 'park_factor_SLG', 'park_factor_OBP','next_opp',
         'next_home', 'next_park_factor_SLG', 'next_park_factor_OBP', 'pts',
         'park_ratio_OBP','park_ratio_SLG','Date'], inplace=True, axis=1)


#add p suffix to indicate pitcher        
df2.rename(columns=dict([[col,col+'_p'] for col in df2.columns if col not in ['next_game','next_team','p','year']]), inplace=True)

df = df.merge(df2.drop('year', axis=1), how='left', on=['next_game','next_team'], validate='m:1')
df = df[df['start'].apply(lambda x: x.date()>datetime.date(2008,1,1))]
#df2_cols = ['HBP_avg', 'BB_avg', 'S_avg', 'D_avg', 'T_avg', 'HR_avg','H_avg', 'TB_avg', 'SO_avg', 'RBI_avg', 'R_avg', 'CS_avg', 'SB_avg','OBP_avg','SLG_avg','OPS_avg','next_park_factor_OBP', 'park_factor_OBP_avg','next_park_factor_SLG', 'park_factor_SLG_avg']
#df2.rename(columns=dict([[col,col+'_p'] for col in df2_cols]), inplace=True)

#use_cols = ['pitcher','pts_500_p','next_game','HBP_avg_p', 'BB_avg_p', 'S_avg_p', 'D_avg_p', 'T_avg_p', 'HR_avg_p', 'H_avg_p', 'TB_avg_p', 'SO_avg_p', 'RBI_avg_p', 'R_avg_p', 'CS_avg_p', 'SB_avg_p','OBP_avg_p','SLG_avg_p','OPS_avg_p', 'park_factor_SLG_avg_p', 'park_factor_OBP_avg_p', 'next_park_factor_SLG_p', 'next_park_factor_OBP_p', 'whip', 'whip_SO']

#df = df.merge(df2[use_cols], how='left', on=['pitcher','next_game'], validate='m:1')


#Get pitcher's first start date to verify missing pitcher data
SQL = '''SELECT t2.id, t2.DATE, game_id, if(pitcher1=t2.id, 0, 1) AS home, if(pitcher1=t2.id, team0, team1) AS team FROM 
(SELECT id, MIN(DATE) AS DATE FROM 
(SELECT pitcher0 AS id, DATE FROM games
UNION
SELECT pitcher1 AS id, DATE FROM games) t
GROUP BY id) t2

LEFT JOIN games g ON (g.pitcher0 = t2.id or g.pitcher1 = t2.id) AND g.date = t2.date'''

p = pd.read_sql_query(SQL, con)

df['year'] = df['start'].apply(lambda x: x.year)


miss = df[df['p'].isna()][['next_game','next_team','year']]
miss.columns = ['game','team','year']
p.rename(columns={'game_id':'game'}, inplace=True)

p['first_start'] = 1
#confirm that missing pitcher info was pitcher's first start
miss = miss.merge(p[['game','team','first_start']], how='left', on=['game','team'], validate='m:1')

#incorrect starter listed so no pitching stats, remove from batting data (game was not pitcher's first start)
errs = miss[miss['first_start'].isna()][['game','team']].value_counts()
errs = pd.DataFrame([[x,y] for x,y in errs.index], columns=['next_game','next_team'])
errs['remove'] = 1
df = df.merge(errs, how='left', on=['next_game','next_team'], validate='m:1')
df = df[df['remove'].isna()]
df.drop(['p','remove'], inplace=True, axis=1)


df['first_time_pitcher'] = np.where(df['IP_avg_p'].isna(), 1, 0)

for col in df.columns[df.isna().sum()>0]:
    df[col].fillna(df[col].quantile(0.25), inplace=True)


df['ops x pts_500 parkadj'] = df['OPS_avg_parkadj_p'] * df['pts_500_parkadj']
df['ops x pts_500 orderadj'] = df['OPS_avg_parkadj_p'] * df['pts_500_parkadj'] / df['order']
df['ops23 x pts_500'] = df['OPS_avg_parkadj_p']**(2/3) * df['pts_500_parkadj']
df['ops x pts_500 order15'] = df['OPS_avg_parkadj_p']**(2/3) * df['pts_500_parkadj'] / df['order']**0.15

df['pts_BxP'] = df['pts_500_parkadj'] * df['pts_500_parkadj_p']
df['SLG_BxP'] = df['SLG_avg_parkadj'] * df['SLG_avg_parkadj_p']
df['OPS_BxP'] = df['OPS_avg_parkadj'] * df['OPS_avg_parkadj_p']
df['whip_SO_BxP'] = df['whip_SO_parkadj_p'] * df['whip_SO_B_parkadj']

df.to_pickle('ml_data.pkl')
