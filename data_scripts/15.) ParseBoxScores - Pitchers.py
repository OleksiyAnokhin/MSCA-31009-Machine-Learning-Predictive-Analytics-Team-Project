# -*- coding: utf-8 -*-
"""
Created on Mon Nov  9 19:01:41 2020

@author: mthom
"""

import pandas as pd
from csv import reader
from sqlalchemy import create_engine
import re   
from password import password
import numpy as np
import datetime

pd.options.display.max_columns = 50


teams = ['ANA','ARI','ATL','BAL','BOS','CHA','CHN','CIN','CLE','COL','DET','HOU','KCA','LAN','MIA','MIL','MIN','NYA','NYN','OAK','PHI','PIT','SDN','SEA','SFN','SLN','TBA','TEX','TOR','WAS']
leagues = ['EVA', 'EVN', 'EVN', 'EVA', 'EVA', 'EVA', 'EVN','EVN','EVA','EVN','EVA','EVA','EVA','EVN','EVN','EVN','EVA','EVA','EVN','EVA','EVN','EVN','EVN','EVA','EVN','EVN','EVA','EVA','EVA','EVN']

league = dict(zip(teams,leagues))


for year in range(2006, 2020):
    print(year)
    for i, team_abbr in enumerate(teams):
        if i>=0:
            
            if team_abbr == 'HOU' and year<2013:
                league[team_abbr] = 'EVN'
            elif team_abbr == 'MIA' and year<2012:
                team_abbr = 'FLO'
                league[team_abbr] = 'EVN'
                
            #print(team_abbr, i)
            file = open('./data/{0}/{0}{1}.BOX'.format(year, team_abbr), 'r')
            raw_data = []
            is_pitcher_data = False
            date_type = re.compile('[0-9]*/[0-9]*/[0-9]*')
            
            for x in reader(file):
                
                if len(x):
                    if 'Game of' in x[0]:
                        date = re.search(date_type, x[0]).group()
                        date = pd.to_datetime(date).date( )
                    if 'IP  H  R ER BB SO' in x[0]:
                        is_pitcher_data = True
                        team = ''.join(x[0].split()[:-6])
                    if is_pitcher_data==True and 'IP  H  R ER BB SO' not in x[0] and x[0].strip()[0]!='*' and x[0].strip()[0] != '+' and x[0].strip()[0] != '#':
                        
                        #print('x[0]:', x[0])
                        #print('x:', x)
                        #raise Exception()
                        for char in ['*','+','#']:
                            x[0] = x[0].replace(char, '')
                        temp = x[0].split()
                        temp.extend([date])
                        temp.extend([team])
                        raw_data.extend([temp])
                    #    print(temp)
                     #   ab = x[0]
                if len(x)==0 and is_pitcher_data==True:
                    is_pitcher_data=False
            
            
            data = []
            for x in raw_data:
                data.extend([[' '.join(x[:-8]), x[-8], x[-7], x[-6], x[-5], x[-4], x[-3], x[-2], x[-1]]])
                
                
            df = pd.DataFrame(data, columns=['name','IP','H','R','ER','BB','SO','date','team'])
            
            df['W'] = df['name'].apply(lambda x: 1 if '(W)' in x else 0)
            df['L'] = df['name'].apply(lambda x: 1 if '(L)' in x else 0)
            
            
            df['IP'] = df['IP'].apply(float)
            
            for col in ['H','R', 'ER','BB','SO']:
                df[col] = df[col].apply(int)
                
            
            df['name'] = df['name'].apply(lambda x: x[0:-4] if x[-3:]=='(W)' or x[-3:]=='(L)'  or x[-3:]=='(S)' else x)
            
            df['QS'] = df.apply(lambda df: 1 if df['IP']>=6 and df['ER']<=3 else 0, axis=1)
            
            df['fanduel_points'] = 6*df['W'] + 4*df['QS'] - 3 *df['ER'] +3 * df['SO'] + df['IP'].apply(int) * 3  + df['IP']%1*10 
            df['fanduel_points'] = df['fanduel_points'].round(0)
            
            
            pw = password()
            con = create_engine('mysql+mysqlconnector://{0}:{1}@{2}:3306/mlb_db'.format(pw.user, pw.password, pw.host))
            SQL = '''SELECT id, team, date FROM
                    (SELECT P AS id, opp AS team, DATE(start) AS date FROM at_bats) t
                    GROUP BY id, team, date'''
            
            team_a = ['St.Louis', 'Milwaukee', 'Chicago', 'Cincinnati', 'Miami', 'Pittsburgh', 'Cleveland', 'Arizona', 'NewYork', 'Philadelphia', 'KansasCity', 'Minnesota', 'Atlanta', 'LosAngeles', 'Washington', 'Colorado', 'SanDiego', 'SanFrancisco', 'Detroit', 'Anaheim', 'Oakland', 'Boston', 'Baltimore', 'Houston', 'TampaBay', 'Texas', 'Toronto', 'Seattle', 'Florida']
            
            #if national league, uses cubs and mets for Chicago and New York, else sox and yankees
            if league[team_abbr] == 'EVN':
                team_b = ['SLN', 'MIL', 'CHN', 'CIN','MIA','PIT','CLE','ARI','NYN','PHI','KCA','MIN', 'ATL','LAN','WAS','COL','SDN','SFN','DET','ANA','OAK','BOS','BAL','HOU','TBA','TEX','TOR','SEA', 'FLO']
            else:
                team_b = ['SLN', 'MIL', 'CHA', 'CIN','MIA','PIT','CLE','ARI','NYA','PHI','KCA','MIN', 'ATL','LAN','WAS','COL','SDN','SFN','DET','ANA','OAK','BOS','BAL','HOU','TBA','TEX','TOR','SEA','FLO']
            
            team_dict = dict(zip(team_a, team_b))
            
            df['full_team'] = df['team']
            df['team'] = df['team'].map(team_dict)
            
            alt_teams = [['CHN','CHA'], ['NYN','NYA']]
            
            SQL = '''SELECT t2.id, t2.team, t2.date, fname, lname FROM
            
            (SELECT id, team, date FROM
            (SELECT P AS id, opp AS team, DATE(start) AS date FROM at_bats) t
            
            GROUP BY id, team, date) t2
            
            LEFT JOIN
            id ON t2.id = id.retro_id'''
            
            ID = pd.read_sql_query(SQL, con)
            
            
            ID['name'] = ID['lname'] + ' ' + ID['fname'].str[0]
            
            lenCheck = len(df)
            df = df.merge(ID[['id','name','date','team']], how='left', on=['name','date','team'])
            if lenCheck!=len(df):
                raise Exception('merge error')
                
            
            df['id'] = np.where( (df['name']=='Vazquez F') & (df['team']=='PIT'), 'rivef001', df['id'])
            df['id'] = np.where( (df['name']=='St. John L') & (df['team']=='TEX'), 'stjol001', df['id'])
            df['id'] = np.where( (df['name']=='Montgomery M') & (df['team']=='CHA'), 'montm002', df['id'])
            
            
            dfx = df[df['id'].notnull()].copy()
            dfy = df[df['id'].isnull()].copy()
            
            dfy['partial'] = dfy['name'].apply(lambda x: x.replace(' ','').lower())
            dfy['partial'] = dfy['partial'].apply(lambda x: x.replace("'",""))
            dfy['partial'] = dfy['partial'].apply(lambda x: x[0:4] + x[-1] if len(x)>=5 else x[:-1]+ '-'*(5-len(x)) + x[-1])
            dfy.drop('id', inplace=True, axis=1)
            
            ID['partial'] = ID['id'].str[0:5]
            
            lenCheck2 = len(dfy)
            dfy = dfy.merge(ID[['partial','date','team','id']], how='left', on=['partial','date','team' ])
            
            #R Ortiz Disambiguation
            #Found in SQL:
            #SELECT pitcher, Opponent AS team, SUM(PA), SUM(AB), SUM(H) FROM mlb WHERE DATE(game_start) = '2010-4-17' AND (pitcher = 'ortir001' OR pitcher = 'ortir002') GROUP BY pitcher
            if year == 2010:
                if len(dfy[(dfy['id']=='ortir002')&(dfy['date']==datetime.date(2010,4,14))&(dfy['H']==3)]):
                    dfy = dfy[dfy.index!=dfy[(dfy['id']=='ortir002')&(dfy['date']==datetime.date(2010,4,14))&(dfy['H']==3)].index[0]]
                if len(dfy[(dfy['id']=='ortir001')&(dfy['date']==datetime.date(2010,4,14))&(dfy['H']==0)]):
                    dfy = dfy[dfy.index!=dfy[(dfy['id']=='ortir001')&(dfy['date']==datetime.date(2010,4,14))&(dfy['H']==0)].index[0]]
                if len(dfy[(dfy['id']=='ortir002')&(dfy['date']==datetime.date(2010,4,17))&(dfy['R']==0)]):
                    dfy = dfy[dfy.index!=dfy[(dfy['id']=='ortir002')&(dfy['date']==datetime.date(2010,4,17))&(dfy['R']==0)].index[0]]
                if len(dfy[(dfy['id']=='ortir001')&(dfy['date']==datetime.date(2010,4,17))&(dfy['R']==2)]):
                    dfy = dfy[dfy.index!=dfy[(dfy['id']=='ortir001')&(dfy['date']==datetime.date(2010,4,17))&(dfy['R']==2)].index[0]]
                if len(dfy[(dfy['id']=='ortir001')&(dfy['date']==datetime.date(2010,4, 5))&(dfy['R']==1)]):
                    dfy = dfy[dfy.index!=dfy[(dfy['id']=='ortir001')&(dfy['date']==datetime.date(2010,4, 5))&(dfy['R']==1)].index[0]]
                if len(dfy[(dfy['id']=='ortir002')&(dfy['date']==datetime.date(2010,4, 5))&(dfy['R']==0)]):
                    dfy = dfy[dfy.index!=dfy[(dfy['id']=='ortir002')&(dfy['date']==datetime.date(2010,4, 5))&(dfy['R']==0)].index[0]]
                if len(dfy[(dfy['id']=='ortir001')&(dfy['date']==datetime.date(2010,4, 7))&(dfy['R']==1)]):
                    dfy = dfy[dfy.index!=dfy[(dfy['id']=='ortir001')&(dfy['date']==datetime.date(2010,4, 7))&(dfy['R']==1)].index[0]]
                if len(dfy[(dfy['id']=='ortir002')&(dfy['date']==datetime.date(2010,4, 7))&(dfy['R']==0)]):
                    dfy = dfy[dfy.index!=dfy[(dfy['id']=='ortir002')&(dfy['date']==datetime.date(2010,4, 7))&(dfy['R']==0)].index[0]]
            
            if year == 2019:
                if len(dfy[(dfy['id']=='scott003')&(dfy['date']==datetime.date(2019, 9, 24))&(dfy['R']==2)]):
                    dfy = dfy[dfy.index!=dfy[(dfy['id']=='scott003')&(dfy['date']==datetime.date(2019, 9, 24))&(dfy['R']==2)].index[0]]
                if len(dfy[(dfy['id']=='scott004')&(dfy['date']==datetime.date(2019, 9, 24))&(dfy['R']==0)]):
                    dfy = dfy[dfy.index!=dfy[(dfy['id']=='scott004')&(dfy['date']==datetime.date(2019, 9, 24))&(dfy['R']==0)].index[0]]
                    
            if year == 2015:
                dfy = dfy[(dfy['id']!='ramij002')|(dfy['date']!=datetime.date(2015,9,24))|(dfy['R']!=5)]
                dfy = dfy[(dfy['id']!='ramij004')|(dfy['date']!=datetime.date(2015,9,24))|(dfy['R']!=0)]
                dfy = dfy[(dfy['id']!='ramij002')|(dfy['date']!=datetime.date(2015,9,8))|(dfy['R']!=1)]
                dfy = dfy[(dfy['id']!='ramij004')|(dfy['date']!=datetime.date(2015,9,8))|(dfy['R']!=0)]
                dfy = dfy[(dfy['id']!='ramij002')|(dfy['date']!=datetime.date(2015,9,13))|(dfy['BB']!=1)]
                dfy = dfy[(dfy['id']!='ramij004')|(dfy['date']!=datetime.date(2015,9,13))|(dfy['BB']!=0)]
    
                dfy = dfy[(dfy['id']!='ramij004')|(dfy['date']!=datetime.date(2015,9,19))|(dfy['H']!=1)]
                dfy = dfy[(dfy['id']!='ramij002')|(dfy['date']!=datetime.date(2015,9,19))|(dfy['H']!=4)]
            
            if lenCheck2 != len(dfy):
                raise Exception('merge error')
            

            
            
            #Jake Diekman pitched for Royals on actual 5/19/19 then traded to OAK and pitched in their makeup game
            #that was originally scheduled for 5/19/19
            if year == 2019:
                dfy = dfy[(dfy['id']!='diekj001')|(dfy['date']!=datetime.date(2019,5,19))|(dfy['team']!='OAK')]
            
            dup = dfy[['partial','date','name']].groupby(['partial','date']).count().copy().reset_index()
            dup = dup[dup['name']>1]
            
            if len(dfy) + len(dfx) - lenCheck != 0 :
                raise Exception('merge error')
            
            dfz = dfy[dfy['id'].isnull()].copy()
            
            if len(dfz)>0:
                dfy = dfy[dfy['id'].notnull()]
                if league[team_abbr] == 'EVA':
                    dfz['team'] = np.where(dfz['team']=='NYA', 'NYN', dfz['team'])
                    dfz['team'] = np.where(dfz['team']=='CHA', 'CHN', dfz['team'])
                else:
                    dfz['team'] = np.where(dfz['team']=='NYN', 'NYA', dfz['team'])
                    dfz['team'] = np.where(dfz['team']=='CHN', 'CHA', dfz['team'])
                
                
                dfz.drop('id', inplace=True, axis=1)
                lenCheck3 = len(dfz)
                dfz = dfz.merge(ID[['partial','date','team','id']], how='left', on=['partial','date','team' ])
                if lenCheck3 != len(dfz):
                    raise Exception('merge error')
                
                dfz['id'] = np.where(pd.isnull(dfz['id'])&(dfz['name']=='Miller A'), 'milla002', dfz['id'])
                
                def final_try(df):
                    if pd.notnull(df['id']):
                        return df['id']
                    ids = ID[(ID['partial']==df['partial'])&(ID['team']==df['team'])]['id'].unique()
                    if len(ids)==1:
                        return ids[0]
                    ids = ID[(ID['partial']==df['partial'])]['id'].unique()
                    if len(ids)==1:
                        return ids[0]
                    return np.nan
                
                dfz['id'] = dfz.apply(final_try, axis=1)
                dfz['id'] = np.where( (dfz['name']=='Casilla S') & ((dfz['team']=='OAK')|(dfz['team']=='SFN')), 'garcj002', dfz['id'])

                dfz['id'] = np.where( (dfz['name']=='Ramirez N') & (dfz['team']=='CHA')&(dfz['date']==datetime.date(2015,4,15)), 'ramin001', dfz['id'])                                     
                dfz.loc[(dfz['id']=='ramin001')&(dfz['team']=='CHA'), 'team'] = 'CHN'
                
                if dfz['id'].isnull().sum()>0:
                    raise Exception('There are unmatched ids in DataFrame dfz')
                
                if len(dfx) + len(dfy) + len(dfz) != lenCheck:
                    raise Exception('split error')
            
                    
            if dfy['id'].isnull().sum()>0:
                raise Exception('There are unmatched ids in DataFrame dfy')
                
            dfy.drop('partial',inplace=True,axis=1)
            df = dfy.append(dfx)
            
            if len(dfz):
                dfz.drop('partial',inplace=True,axis=1)
                df = df.append(dfz)
                
            if len(df)!=lenCheck:
                raise Exception('error')
                
            engine = create_engine('mysql+mysqlconnector://{0}:{1}@{2}/mlb_db'.format(pw.user,pw.password,pw.host), echo=False)
            df.sort_values(['id','date'], inplace=True)
            df.reset_index(inplace=True, drop=True)
            df['number'] = 0
            df['number'] = np.where( (df['id']==df['id'].shift(-1)) & (df['date']==df['date'].shift(-1)), 1, df['number'])
            df['number'] = np.where( (df['id']==df['id'].shift(1)) & (df['date']==df['date'].shift(1)), 2, df['number'])
            
            #this pitcher was traded during 2019 and pitched in a game that started when he was with the other day but resumed once he was traded.
            #he pitched in this game once it was resumed, and thus pitched for two teams on the same day
            #since team is rightfully not included in the unique key, this violated the unique key constraints in SQL
            #there for, switching the game# to 1 to get SQL to include this record. Since game# is less important than having date correct
            if year == 2019:
                df['number'] = np.where((df['date']==datetime.date(2019,5,19))&(df['id']=='diekj001'), 1, df['number'])
            
            df[['id','team','date','IP','H','R','ER','BB','SO','W','L','QS','fanduel_points','number']].to_sql(con=engine, index=False, if_exists='append', name='pitcher_points', chunksize=1000)
            print('{0} {1} {2}'.format(team_abbr, i, df['date'].max()))
            engine.dispose()
