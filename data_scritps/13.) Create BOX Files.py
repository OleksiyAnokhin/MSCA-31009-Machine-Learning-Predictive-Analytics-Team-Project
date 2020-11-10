# -*- coding: utf-8 -*-
"""
Created on Mon Nov  9 19:00:33 2020

@author: mthom
"""

import os
import zipfile

teams = ['ANA','ARI','ATL','BAL','BOS','CHA','CHN','CIN','CLE','COL','DET','HOU','KCA','LAN','MIA','MIL','MIN','NYA','NYN','OAK','PHI','PIT','SDN','SEA','SFN','SLN','TBA','TEX','TOR','WAS']
leagues = ['EVA', 'EVN', 'EVN', 'EVA', 'EVA', 'EVA', 'EVN','EVN','EVA','EVN','EVA','EVA','EVA','EVN','EVN','EVN','EVA','EVA','EVN','EVA','EVN','EVN','EVN','EVA','EVN','EVN','EVA','EVA','EVA','EVN']
cdir = os.getcwd()


    
for yr in range(2006, 2020):

    print(yr)

    path = '{0}/data/{1}'.format(cdir, yr)
    file = '{0}/{1}eve.zip'.format(path, yr)
    

    with zipfile.ZipFile(file, 'r') as zip_ref:
        zip_ref.extractall(path)

    #copy BOX.EXE into directory
    os.system('copy {0}\\data\\BOX.EXE {0}\\data\\{1}\\BOX.EXE'.format(cdir, yr))
    
    for team, league in list(zip(teams,leagues)):
        if yr<2013 and team=='HOU':
            league = 'EVN'
        elif yr<2012 and team=='MIA':
            team='FLO'
            
        os.chdir('{0}\\data\\{1}\\'.format(cdir, yr))
        os.system('box -y {0} {0}{1}.{2} > {0}{1}.BOX'.format(yr,team,league))