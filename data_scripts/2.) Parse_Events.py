# -*- coding: utf-8 -*-
"""
Created on Mon Nov  2 14:49:53 2020

@author: mthom
"""


import zipfile as zp
import pandas as pd
from csv import reader
import numpy as np
from parse_event_codes import plate_appearance, at_bat, sac, hbp, walks, single, double, triple, home_run


for year in list(range(2006,2020)):
    path = '.\\data\\{}\\'.format(year)
    away = ''
    home = ''
    team = ''
    game_date = ''
    game_start = ''
    opponent = ''
    data = []
    pitcher = ''
    home_fielders = list(range(10))
    away_fielders = list(range(10))
    fielders = []
    pitcher, catcher, first_basemen, second_baseman, third_baseman, short_stop, left_fielder, center_fielder, right_fielder, designated_hitter='','','','','','','','','',''
    errorchecking = ''
        
    ct=0

    print(year)
    filepath = '.\\data\\{0}\\{0}eve.zip'.format(year)
    zfile = zp.ZipFile(filepath, mode='r')
    #Event files have file extensions EVA (American League Teams) or EVN (National League Teams)
    file_list = [file for file in zfile.namelist() if file[-3:-1]=='EV']
    
    #loop through Event files for given year
    for team_file in file_list:
        text = zfile.read(team_file)
        text = str(text)
        text = text.split(sep=r'\n')
        
        for line in reader(text):
            line = [x.replace(r'\r', '') for x in line]
            
            #represents a new game
            if line[0] == 'b\'id' or line[0]=='id':
                game_id = line[1]
                ct+=1
                
                evt_num = -1
                batter_num = -1
                batting = 0
                inning = 1
                batter = 'event'
            if line[0] == 'info' and line[1]=='temp':
                temp = line[2]
            if line[0] == 'info' and line[1]=='date':
                game_date = line[2]
            if line[0] == 'info' and line[1]=='starttime':
                game_start = pd.to_datetime(game_date +' '+line[2])
            if line[0] == 'info' and line[1]=='visteam':
                away = line[2]
            if line[0] == 'info' and line[1]=='hometeam':
                home = line[2]
              
            
            #rosters
            if line[0] == 'start':
                position = int(line[5].replace(r'\r', '')) % 10
                
                if int(line[3]) == 1:
                    home_fielders[position] = line[1]
                else:
                    away_fielders[position] = line[1]        
            
            #plays (NP means no play)
            if line[0]=='play' and line[6]!='NP':
                if batting == int(line[2]) and inning == int(line[1]):
                    evt_num += 1
                    if batter != line[3]:
                        batter_num += 1
                else:
                    batter_num = 0
                    evt_num = 0
                    
                batting = int(line[2])
                inning = int(line[1])
                batter = line[3]
                
            if line[0]=='play' and line[2] == '1':
                    team = home
                    opponent = away
                    #fielders during at-bat
                    [designated_hitter, pitcher, catcher, first_basemen, second_baseman, third_baseman, short_stop, left_fielder, center_fielder, right_fielder] = away_fielders
                    
            else:
                    team = away
                    opponent = home
                    #fielders during at-bat
                    [designated_hitter, pitcher, catcher, first_basemen, second_baseman, third_baseman, short_stop, left_fielder, center_fielder, right_fielder] = home_fielders
    
            #record player substitution
            if line[0]=='sub' and len(line[5]) == 1:
                if line[3] =='1':
                    home_fielders[int(line[5])] = line[1]
                else:
                    away_fielders[int(line[5])] = line[1]
            if line[0] == 'play':     
                evt = [game_id, batter_num, evt_num] + line[1:] + [team, team_file, game_start, opponent, designated_hitter, pitcher, catcher, first_basemen, second_baseman, third_baseman, short_stop, left_fielder, center_fielder, right_fielder]
                data.append(evt)
                
    cols = ['game_id','batter_num','evt_num','inning', 'home', 'id', 'count','pitches', 'event', 'team', 'file', 'start', 'opp', 'dh', 'P', 'C', '1B', '2B', '3B', 'SS', 'LF', 'CF', 'RF']
    df = pd.DataFrame(data, columns=cols)
    
    for col in ['home','inning']:
        df[col] = df[col].apply(int)
    
    df.index.name = 'At Bat'
    df = df[df['event']!='NP']
    
    
    functions = [plate_appearance, at_bat, sac, hbp, walks, single, double, triple, home_run]
    fields = ['PA', 'AB', 'SAC', 'HBP', 'BB', 'S', 'D', 'T', 'HR']
    
    for func, field in zip(functions, fields):
        df[field] = df['event'].apply(func)
    
    df['TB'] = df['S'] + df['D'] * 2 + df['T'] * 3 + df['HR'] * 4
    
    df['year'] = df['start'].dt.year
    
    #WP is a wild pitch, some x records are W+WP which means the batter walked on a wild pitch. Other records are just WP, which means the at-bat has not finished
    df.loc[df['event'].str.contains('WP')&(df['BB']==0)&(df['event'].str[0]!='K'), 'PA'] = 0
    df.loc[df['event'].str.contains('WP')&(df['event'].str[0]!='K'), 'AB'] = 0
    
    
    df['H'] = df['S'] + df['D'] + df['T'] + df['HR']
    if df[['S','D','T','HR']].sum().sum() - df['H'].sum() !=0:
        raise Exception('hits were not calculated correctly')
    
    #strikeouts 
    df['SO'] = np.where(df['event'].str[0]=='K', 1, 0)

    #Balks advance runners batter remains at-bat; PO is pick-off attempt; CS is caught-stealing
    #C/E1 or C/E3 are used when the pitcher or first baseman are called for interfering with the batter putting him on first without being charged with an at bat
    #note than intereference counts as a PA but does not count against OBP, so I've excluded for simplicity
    #OA is other advance - baserunner advanced for unknown reason
    for evt in ['BK','PO','CS', 'OA']:
        for col in ['AB','PA']:
            df[col] = np.where(df['event'].str[0:2]==evt, 0, df[col])
    
    #walks, sacrifices and hit by pitch not at-bats
    df.loc[(df['SAC']==1)|(df['HBP']==1)|(df['BB']==1), 'AB'] = 0
    
    
    df.to_pickle('./data/{0}/{0}_events.pkl'.format(year))
