# -*- coding: utf-8 -*-
"""
Created on Tue Nov  3 14:29:20 2020

@author: mthom
"""

# attribute run to the baserunners at-bat record. 
#by finding largest event number for the baserunner 
#that is less than the event number of the at-bat
#necessary because some baserunners will have multiple
#at-bats in a single inning


import pandas as pd
import numpy as np


for year in range(2006, 2020):
    print(year)
    df = pd.read_pickle('./data/{0}/{0}_BEVENT.pkl'.format(year))
    
    #remove first record if two records in a row for same at-bat
    #this happens if an event occured that did not finish the at-bat
    #such as a pick-off attempt, wild pitch, etc.
    #(full list: ['SB','WP','PO','PB','CS','DI','BK','OA','FL']])
    df['runner_scored'] = 0
    for runner in ['1st','2nd','3rd']:
        df['runner_scored'] += np.where(df['runner on {} dest'.format(runner)]>=4, 1, 0)
    df['runner_scored'] += np.where(df['batter dest']>=4, 1, 0)
    df = df[(df['runner_scored']>0) |(df['game_id']!=df['game_id'].shift(-1))|(df['id']!=df['id'].shift(-1))|(df['inning']!=df['inning'].shift(-1))|(df['Home']!=df['Home'])]
    
    df['R'] = np.where(df['batter dest']>=4, 1, 0)
    
    
    run1 = df[df['runner on 1st dest']>=4].copy()
    run2 = df[df['runner on 2nd dest']>=4].copy()
    run3 = df[df['runner on 3rd dest']>=4].copy()
    
    df['idx'] = range(len(df))
    
    missing = []
    
    for run, col in zip([run1, run2, run3],['first runner','second runner','third runner']):
        run['run_flag'] = 1
        run['id'] = run[col]
        run['evt_num_run'] = run['evt_num']
    
        lenCheck = len(df)            
        df = df.merge(run[['game_id','id','inning','Home', 'run_flag','evt_num_run']],
                      how='left', on=['game_id','id','inning','Home'])
        
        
        #records where batter had another plate appearance 
        #in same inning after scoring (make sure to attribute to correct plate-appearnce)    
        df['evt_num_run'] = np.where((df['evt_num']>df['evt_num_run']), np.nan, df['evt_num_run'])
        
        #attribut run to most recent at bat
        df['max_evt_num_run'] = df[['game_id','id','inning','Home','evt_num_run','evt_num']].groupby(['game_id','id','inning','Home','evt_num_run']).transform('max')
        
        x = df[(df['max_evt_num_run'] == df['evt_num'])].copy()
        x.reset_index(inplace=True)
        x[['id','game_id','inning','evt_num','batter_num','evt_num_run','max_evt_num_run']][(x['evt_num_run']==x['evt_num_run'].shift(1))&(x['id']==x['id'].shift(1))&(x['inning']==x['inning'].shift(1))&(x['game_id']==x['game_id'].shift(1))].head(10)
        
        df['x'] = 1
        
        df['R'] = np.where(df['max_evt_num_run']==df['evt_num'], 1, df['R'])
        df['ct']=df[['game_id','id','inning','Home','evt_num','away']].groupby(['game_id','id','inning','Home','evt_num']).transform('count')
        
        #remove duplicates
        df = df[~((df['ct']==2)&(df['R']==0))]
        #check that duplicates were removed correctly
        if len(df)!=lenCheck:
            raise Exception('merge error')
        
        lenCheck = len(run)
        x = run.merge(df[df['max_evt_num_run']==df['evt_num']][['game_id','id','inning','Home','x']], how='left', on=['game_id','id','inning','Home'])
        
        missing += [x[x['x'].isnull()][['game_id','inning','Home','id']].copy()]
        df.drop(['run_flag', 'evt_num_run', 'max_evt_num_run', 'x', 'ct'], axis=1, inplace=True)

    diff = df['runner_scored'].sum() - ( df['R'].sum() + sum([len(x) for x in missing]))
    if diff:
        print('off by {0}'.format(diff))
       
    df.drop('runner_scored', inplace=True, axis=1)
    
    #add to EVT file
    evt = pd.read_pickle('./data/{0}/{0}_events.pkl'.format(year))
    
    lenCheck = len(evt)
    df.rename(columns={'Home':'home','Event':'event'}, inplace=True)
    evt = evt.merge(df[['game_id','inning','home','id','evt_num','event','RBI','R','batter hand','pitcher hand','lineup position']], how='left', on=['game_id','inning','home','id','evt_num','event'], validate='1:1')
    
    if evt['R'].sum()!=df['R'].sum():
        raise Exception('merge error')
    
    evt['RBI'].fillna(0, inplace=True)
    evt['R'].fillna(0, inplace=True)
    
    #add runs from pinch runners
    missing = missing[0].append(missing[1]).append(missing[2])
    missing['R'] = 1
    missing['event'] = 'Pinch Runner Scores'
    
    #add date
    dates = evt[['start','game_id']].groupby('game_id').min().reset_index()
    missing = missing.merge(dates, how='left', on=['game_id'], validate='m:1')
    
    evt = evt.append(missing)
    evt.to_pickle('./data/{0}/{0}_events_with_runs.pkl'.format(year))
    