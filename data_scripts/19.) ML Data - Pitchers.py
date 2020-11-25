# -*- coding: utf-8 -*-
"""
Created on Sat Oct 19 10:41:15 2019

@author: mthom
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from password import password


averaging_period = 500
pd.options.display.max_columns = 50


pw = password()
con = create_engine('mysql+mysqlconnector://{0}:{1}@localhost/mlb_db'.format(pw.user, pw.password))


SQL = 'Select game_id game, start, P id, team as opp, opp as team, 1-home as home, inning, event, PA, AB, SAC, HBP, BB, S, D, T, HR, H, TB, SO, RBI, R, CS, SB from at_bats where P is not null Order By P Asc, start Asc, inning Asc'


df = pd.read_sql_query(SQL, con)
df['date'] = df['start'].apply(lambda x: x.date())

#only include Plate Appearances or Catcher Intereference
df = df[(df['PA']==1)|(df['event'].str[0:2]=='C/')].copy()
df.reset_index(inplace=True, drop=True)

#Only include starters
SQL = '''SELECT game_id as game, p as id FROM 
(SELECT game_id, pitcher0 AS p FROM games) t
UNION
SELECT game_id, pitcher1 AS p FROM games'''

starters = pd.read_sql_query(SQL, con)
starters['starter'] = 1

df = df.merge(starters, how='left', on=['game','id'], validate='m:1')
df = df[df['starter']==1]
df['game_num'] = df['game'].apply(lambda x: int(x[3:]))
df['new_game'] = np.where( (df['game_num']!= df['game_num'].shift(1))|(df['id']!= df['id'].shift(1)), 1, 0)


   
#only keep last record for each game (rather than each at-bat)
df.sort_values(['id', 'date', 'inning'], inplace=True)
df.reset_index(inplace=True, drop=True)
#df.fillna(0, inplace=True)

cols = ['PA', 'AB', 'SAC', 'HBP', 'BB', 'S', 'D', 'T', 'HR', 'H', 'TB', 'SO', 'RBI', 'R', 'CS', 'SB']

df = df[['date','game','id','team','home','opp','PA', 'AB', 'SAC', 'HBP', 'BB', 'S', 'D', 'T', 'HR', 'H', 'TB', 'SO', 'RBI', 'R', 'CS', 'SB']].groupby(['date','game','id','team','home','opp']).sum().reset_index()

####################################################################
pw = password()
SQL = 'Select id, team, date, IP, ER, W, L, QS, number from pitcher_points order by id, date'
points = pd.read_sql_query(SQL, con)
points['IP'] = points['IP']%1*10/3 + points['IP']//1
points['IP'] = points['IP'].round(4)

  
df['Date'] = df['date'].apply(lambda x: pd.to_datetime(x).date())
points['Date'] = points['date']

points.drop(['date'], axis=1, inplace=True)    
df['number'] = df['game'].str[-1]
df['number'] = df['number'].apply(int) 




df = df.merge(points, how='left', on=['id','Date','team','number'], validate='m:1')

df1 = df[df['W'].notnull()].copy()
df2 = df[df['W'].isnull()].copy()

df2.drop(['IP', 'ER', 'W', 'L', 'QS'], axis=1, inplace=True)
points['number'] += 1
df2 = df2.merge(points, how='left', on=['id','Date','team','number'], validate='m:1')

df = df1.append(df2)

df1 = df[df['W'].notnull()].copy()
df2 = df[df['W'].isnull()].copy()

df2.drop(['IP', 'ER', 'W', 'L', 'QS'], axis=1, inplace=True)
points['number'] += 1


df2 = df2.merge(points, how='left', on=['id','team','Date','number'], validate='m:1')
#$df2 = df2[(df2['id']!='scott003')|(df2['Date']!=datetime.date(2019,9,15))|(df2['number']!=3)]

df = df1.append(df2[df1.columns])


#for col, val in [['number', 2], ['IP', 1], ['ER', 0], ['W', 0], ['L', 0], ['QS',0]]:
#    df.loc[(df['id']=='scott004')&(df['Date']==datetime.date(2019,9,15)), col] = val
if df['W'].isna().sum()>10:
    raise Exception()



#############################################


#Rolling can be variable length when the index of the dataframe is a date
#so make a fake date that addes the cumulative amount of innings to it (in days)
#such that a rolling window of 500 days actually corresponds to 500 innnings
#(OR whatever number of innings provides the highest correlation to actual points)
#df.sort_values(['id','date'], inplace=True)
#df.reset_index(inplace=True, drop=True)    
#df['window'] = datetime.date(2010, 1, 1)
#df['window_chg'] = df[['id','IP']].groupby('id').transform('cumsum')
#df['window'] = df.apply(lambda df: df['window'] + datetime.timedelta(days=df['window_chg']), axis=1)
#df['window'] = df['window'].apply(pd.to_datetime)
#df.index = df['window']

cols = ['IP','PA', 'AB', 'SAC', 'HBP', 'BB', 'S', 'D', 'T', 'HR', 'H', 'TB', 'SO', 'RBI', 'R', 'CS', 'SB']


for col in cols:
    df[col+'_avg'] = df[[col,'id']].groupby('id').rolling(100, min_periods=0).sum().reset_index()[col]
    #df[col+'_avg'] = np.where(df['IP_avg']==0, df[col+'_avg'][df[col+'_avg']<np.inf].mean(), df[col+'_avg'])
    #df[col+'_avg'] = df[col+'_avg'] * 9  

#get park factors
park = pd.read_pickle('./park_factors.pkl')
#park.rename(columns={'SLG_ratio':'park_factor_SLG','OBP_ratio':'park_factor_OBP','OPS_ratio':'park_factor_OPS'}, inplace=True)
df['park'] = np.where(df['home']==1, df['team'], df['opp'])


df = df.merge(park[['park','park_factor_SLG','park_factor_OBP']], how='left', on=['park'], validate='m:1')

#df.index = df['window']
for col in ['park_factor_SLG','park_factor_OBP']:
    df[col+'_avg'] = df[[col,'id']].groupby('id').rolling(100, min_periods=0).mean().reset_index()[col]


df.reset_index(inplace=True, drop=True)    


df['whip'] = (df['H_avg'] + df['BB_avg']) / df['IP_avg']
df['whip_SO'] = (df['H_avg'] + df['BB_avg'] - df['SO_avg']) / df['IP_avg']

df.drop(cols, inplace=True, axis=1)

df.sort_values(['id','date'], inplace=True)
for col in ['game','team','opp','home','park_factor_SLG','park_factor_OBP']:
    df['next_{}'.format(col)] = df[['id',col]].groupby('id').shift(-1)
    

#pull in actual points (for next game)
SQL = '''Select game_id game, start, P id, max(home) home, 
    sum(PA) PA, sum(AB) AB, sum(SAC) SAC, sum(HBP) HBP, sum(BB) BB, sum(S) S, sum(D) D, sum(T) T, 
    sum(HR) HR, sum(H) H, sum(TB) TB, sum(SO) SO, sum(RBI) RBI, sum(R) R, sum(CS) CS, sum(SB) SB 
    from at_bats group by game, P Order By P Asc, start Asc, inning Asc'''


game = pd.read_sql_query(SQL, con)


game['pts'] = 3*game['S'] + 6*game['D'] + 9*game['T'] + 12*game['HR']  +  3 * game['BB'] + 3 * game['HBP'] + 3.2 * game['R'] + 3.5 * game['RBI'] + 6 * game['SB']
game.rename(columns={'game':'next_game'}, inplace=True)




df = df.merge(game[['next_game','id','pts']], how='left', on=['next_game','id'], validate='m:1')
    

df['pts_500'] = (3*df['S_avg'] + 6*df['D_avg'] + 9*df['T_avg'] + 12*df['HR_avg']  +  3 * df['BB_avg'] + 3 * df['HBP_avg'] + 3.2 * df['R_avg'] + 3.5 * df['RBI_avg'] + 6 * df['SB_avg']) 

df['OBP_avg'] = (df['H_avg'] + df['BB_avg'] + df['HBP_avg']) 
df['SLG_avg'] = (df['S_avg'] + 2*df['D_avg'] + 3*df['T_avg'] + 4*df['HR_avg'])
df['OPS_avg'] = df['OBP_avg'] + df['SLG_avg']
df['OPS_Adj_avg'] = 1.8*df['OBP_avg'] + df['SLG_avg']

#Park Adjustments
df['park_ratio_OBP'] = df['next_park_factor_OBP']/df['park_factor_OBP_avg']
df['park_ratio_SLG'] = df['next_park_factor_SLG']/df['park_factor_SLG_avg']

df['pts_500_parkadj'] = df['pts_500'] * df['park_ratio_SLG']
df['SLG_avg_parkadj'] = df['SLG_avg'] * df['park_ratio_SLG']
df['OPS_avg_parkadj'] = df['OPS_avg'] * df['park_ratio_SLG']

df['whip_SO_parkadj'] = df['whip_SO'] * df['park_ratio_SLG']
df['whip_parkadj'] = df['whip'] * df['park_ratio_SLG']

df[df['pts'].notnull()].to_pickle('./pitcher.pkl')

#df[df['id']=='kellm003'][['date','game','next_game','team','next_team','next_opp']]