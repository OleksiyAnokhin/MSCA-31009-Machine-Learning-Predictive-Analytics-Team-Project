# -*- coding: utf-8 -*-
"""
Created on Mon Nov  2 17:44:00 2020

@author: mthom
"""


import pandas as pd
import numpy as np
import glob
import os
import time


start = time.time()
cols = ['game_id', 'away', 'inning', 'Home', 'outs', 'balls', 'strikes', 'pitch sequence', 'vis score', 'home score', 'id', 'batter hand', 'res batter', 'res batter hand', 'pitcher', 'pitcher hand', 'res pitcher', 'res pitcher hand', 'catcher', 'first base', 'second base', 'third base', 'shortstop', 'left field', 'center field', 'right field', 'first runner', 'second runner', 'third runner', 'Event', 'leadoff flag', 'pinchhit flag', 'defensive position', 'lineup position', 'event type', 'batter event flag', 'ab flag', 'hit value', 'SH flag', 'SF flag', 'outs on play', 'double play flag', 'triple play flag', 'RBI', 'wild pitch flag', 'passed ball flag', 'fielded by', 'batted ball type', 'bunt flag', 'foul flag', 'hit location', 'num errors', '1st error player', '1st error type', '2nd error player', '2nd error type', '3rd error player', '3rd error type', 'batter dest', 'runner on 1st dest', 'runner on 2nd dest', 'runner on 3rd dest', 'play on batter', 'play on runner on 1st', 'play on runner on 2nd', 'play on runner on 3rd', 'SB for runner on 1st flag', 'SB for runner on 2nd flag', 'SB for runner on 3rd flag', 'CS for runner on 1st flag', 'CS for runner on 2nd flag', 'CS for runner on 3rd flag', 'PO for runner on 1st flag', 'PO for runner on 2nd flag', 'PO for runner on 3rd flag', 'Responsible pitcher for runner on 1st', 'Responsible pitcher for runner on 2nd', 'Responsible pitcher for runner on 3rd', 'New Game Flag', 'End Game Flag', 'Pinch-runner on 1st', 'Pinch-runner on 2nd', 'Pinch-runner on 3rd', 'Runner removed for pinch-runner on 1st', 'Runner removed for pinch-runner on 2nd', 'Runner removed for pinch-runner on 3rd', 'Batter removed for pinch-hitter', 'Position of batter removed for pinch-hitter', 'Fielder with First Putout', 'Fielder with Second Putout', 'Fielder with Third Putout', 'Fielder with First Assist', 'Fielder with Second Assist', 'Fielder with Third Assist', 'Fielder with Fourth Assist', 'Fielder with Fifth Assist', 'Event Num', 'batter_num', 'evt_num']
cdir = os.getcwd()
 
for year in range(2006, 2020):
    print(year)
    path = '{0}/data/{1}/'.format(cdir, year)
    os.chdir(path)
    files = glob.glob('*.{}'.format('bev'))
    df = pd.DataFrame(columns=cols)
    
    for file in files:
        dfx = pd.read_csv('{}'.format(file), header=None)
        dfx.columns = cols[:-2]    
        
        dfx['batter_num'] = 0
        dfx['evt_num'] = 0
        
        dfx.reset_index(inplace=True, drop=True)
        #calculate event_number and batter_number to align with Events file
        dfx['new_inning'] = np.where( (dfx['inning']==dfx['inning'].shift(1)) & (dfx['Home']==dfx['Home'].shift(1)), 0, 1)
        dfx['new_batter'] = np.where( (dfx['id']!=dfx['id'].shift(1)) , 1, 0)
        dfx['new_batter_cumulative'] = dfx['new_batter'].cumsum()
        dfx['new_inning_cumulative'] = dfx['new_inning'].cumsum()
        dfx['min_batter'] = dfx[['new_batter_cumulative','new_inning_cumulative']].groupby('new_inning_cumulative').transform('min')
        dfx['batter_num'] = dfx['new_batter_cumulative'] - dfx['min_batter']
        
        dfx['idx'] = range(len(dfx))
        dfx['min_evt_count'] = dfx[['idx','new_inning_cumulative']].groupby('new_inning_cumulative').transform('min')
        dfx['evt_num'] = dfx['idx'] - dfx['min_evt_count']
        
        dfx.drop(['new_inning','new_batter','new_batter_cumulative','new_inning_cumulative','min_batter','idx','min_evt_count'], inplace=True, axis=1)
        df = df.append(dfx)
        
        
    
    df.to_pickle('{0}_BEVENT.pkl'.format(year))

stop = time.time()
print('--- {0:,.0f} sec ---'.format(stop-start))