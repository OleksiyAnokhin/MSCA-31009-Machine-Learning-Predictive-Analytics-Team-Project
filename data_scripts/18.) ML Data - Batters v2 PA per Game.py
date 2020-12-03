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

pw = password()
con = create_engine('mysql+mysqlconnector://{0}:{1}@localhost/mlb_db'.format(pw.user, pw.password))

SQL = 'Select game_id game, start, home, team, opp, id, inning, event, PA, AB, SAC, HBP, BB, S, D, T, HR, H, TB, SO, RBI, R, CS, SB from at_bats where P is not null Order By id Asc, start Asc, Inning Asc'
df = pd.read_sql_query(SQL, con)

#only include Plate Appearances or Catcher Intereference
df = df[(df['PA']==1)|(df['event'].str[0:2]=='C/')].copy()
df.reset_index(inplace=True, drop=True)

#get park factors
park = pd.read_pickle('./park_factors.pkl')
df['park'] = np.where(df['home']==1, df['team'], df['opp'])

df = df.merge(park[['park','park_factor_SLG','park_factor_OBP']], how='left', on=['park'], validate='m:1')


cols= ['date','number','team0','team1','pitcher0','pitcher1','away1','away2','away3','away4','away5','away6','away7','away8','away9','home1','home2','home3','home4','home5','home6','home7','home8','home9']
colStr = ''
for col in cols:
    colStr = '{0}{1}, '.format(colStr, col)

cols = ['PA', 'AB', 'SAC', 'HBP', 'BB', 'S', 'D', 'T', 'HR', 'H', 'TB', 'SO', 'RBI', 'R', 'CS', 'SB']

df['game_num'] = df['game'].apply(lambda x: int(x[3:]))
df['new_game'] = np.where( (df['game_num']!= df['game_num'].shift(1))|(df['id']!= df['id'].shift(1)), 1, 0)
df['games'] = df[['new_game','id']].groupby('id').rolling(averaging_period, min_periods=0).sum().reset_index()['new_game']

#calculate rolling averages by player
for col in cols:
    df[col+'_avg'] = df[[col,'id']].groupby('id').rolling(averaging_period, min_periods=0).sum().reset_index()[col] / df['games']
   
for col in ['park_factor_SLG','park_factor_OBP']:
    df[col+'_avg'] = df[[col,'id']].groupby('id').rolling(averaging_period, min_periods=0).mean().reset_index()[col]


df['OBP_avg'] = (df['H_avg'] + df['BB_avg'] + df['HBP_avg']) / df['PA_avg']
df['SLG_avg'] = (df['S_avg'] + 2*df['D_avg'] + 3*df['T_avg'] + 4*df['HR_avg']) / df['AB_avg']
df['OPS_avg'] = df['OBP_avg'] + df['SLG_avg']
df['OPS_Adj_avg'] = 1.8*df['OBP_avg'] + df['SLG_avg']


#only keep last record for each game (rather than each at-bat)
df.sort_values(['id', 'start', 'inning'], inplace=True)
df.reset_index(inplace=True, drop=True)
df.fillna(0, inplace=True)
df['idx'] = df.index
df['maxidx'] = df[['game','id','idx']].groupby(['game','id']).transform(max).copy()
df = df[df['idx']==df['maxidx']].copy()
df.drop(cols, inplace=True, axis=1)

df.reset_index(inplace=True, drop=True)   
df.reset_index(inplace=True, drop=True)

for col in ['game','team','opp','home','park_factor_SLG','park_factor_OBP']:
    df['next_{}'.format(col)] = df[['id',col]].groupby('id').shift(-1)
    

colStr = colStr[:-2]
#only include starting lineup
SQL = 'Select {0} from games'.format(colStr)
games = pd.read_sql_query(SQL, con)
games['game'] = games['team1'] + games['date'].map(str) +  games['number'].map(str)
#dont include pitchers in batting data
flat = games[['game','away1']][(games['away1']!=games['pitcher1'])&(games['away1']!=games['pitcher0'])].copy()
flat.columns = ['game','id']
flat['order'] = 1

for i, col in enumerate(['away2','away3','away4','away5','away6','away7','away8','away9','home1','home2','home3','home4','home5','home6','home7','home8','home9'], 1):
    flatx = games[['game',col]][(games[col]!=games['pitcher1'])&(games[col]!=games['pitcher0'])].copy()
    flatx.columns = ['game','id']
    flatx['order'] = i%9+1
    flat = flat.append(flatx)
    
flat.reset_index(inplace=True, drop=True)
flat.columns = ['next_game','id','order']
flat['starter'] = 1

df = df.merge(flat, how='left', on=['next_game','id'], validate='m:1')


df = df[df['starter']==1].copy()
df.drop('starter', axis=1, inplace=True)


df['order_avg'] = df[['order','id']].groupby('id').rolling(averaging_period, min_periods=0).mean().reset_index()['order']
df['next_order'] = df[['id','order']].groupby('id').shift(-1)
df.drop('order', inplace=True, axis=1)    
#pull in actual points (for next game)
SQL = 'Select game_id game, start, id, max(home) home, sum(PA) PA, sum(AB) AB, sum(SAC) SAC, sum(HBP) HBP, sum(BB) BB, sum(S) S, sum(D) D, sum(T) T, sum(HR) HR, sum(H) H, sum(TB) TB, sum(SO) SO, sum(RBI) RBI, sum(R) R, sum(CS) CS, sum(SB) SB from at_bats group by game, id Order By id Asc, start Asc, inning Asc'
game = pd.read_sql_query(SQL, con)


game['pts'] = 3*game['S'] + 6*game['D'] + 9*game['T'] + 12*game['HR']  +  3 * game['BB'] + 3 * game['HBP'] + 3.2 * game['R'] + 3.5 * game['RBI'] + 6 * game['SB']
game.rename(columns={'game':'next_game'}, inplace=True)


df = df.merge(game[['next_game','id','pts']], how='left', on=['next_game','id'], validate='m:1')
    

df['pts_500'] = (3*df['S_avg'] + 6*df['D_avg'] + 9*df['T_avg'] + 12*df['HR_avg']  +  3 * df['BB_avg'] + 3 * df['HBP_avg'] + 3.2 * df['R_avg'] + 3.5 * df['RBI_avg'] + 6 * df['SB_avg']) 


#Park Adjustments
df['park_ratio_OBP'] = df['next_park_factor_OBP']/df['park_factor_OBP_avg']
df['park_ratio_SLG'] = df['next_park_factor_SLG']/df['park_factor_SLG_avg']
df['order_ratio'] = df['next_order']/df['order_avg']

df['pts_500_parkadj'] = df['pts_500'] * df['park_ratio_SLG']
df['SLG_avg_parkadj'] = df['SLG_avg'] * df['park_ratio_SLG']
df['OPS_avg_parkadj'] = df['OPS_avg'] * df['park_ratio_SLG']

df['whip_SO_B'] = (df['H_avg'] + df['BB_avg'] - df['SO_avg']) 
df['whip_SO_B_parkadj'] = df['whip_SO_B'] * df['park_ratio_SLG']

df[df['pts'].notnull()].to_pickle('./batters v2.pkl')
