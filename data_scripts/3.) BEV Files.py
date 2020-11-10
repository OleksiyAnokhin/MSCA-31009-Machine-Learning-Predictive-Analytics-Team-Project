# -*- coding: utf-8 -*-
"""
Created on Mon Nov  2 16:22:15 2020

@author: mthom
"""

#This file runs the BEVENT file from retrosheet.org which 
#extracts additional information from the .EVN/.EVA files

import os
import zipfile

teams = ['ANA','ARI','ATL','BAL','BOS','CHA','CHN','CIN','CLE','COL','DET','HOU','KCA','LAN','MIA','MIL','MIN','NYA','NYN','OAK','PHI','PIT','SDN','SEA','SFN','SLN','TBA','TEX','TOR','WAS','FLO','HOU']
leagues = ['EVA', 'EVN', 'EVN', 'EVA', 'EVA', 'EVA', 'EVN','EVN','EVA','EVN','EVA','EVA','EVA','EVN','EVN','EVN','EVA','EVA','EVN','EVA','EVN','EVN','EVN','EVA','EVN','EVN','EVA','EVA','EVA','EVN','EVN','EVN']
cdir = os.getcwd()

for year in range(2006, 2020):
    print(year)
    file = '{0}\\data\\{1}\\{1}eve.zip'.format(cdir, year)
    path = '{0}\\data\\{1}\\'.format(cdir, year)
    
    with zipfile.ZipFile(file, 'r') as zip_ref:
        zip_ref.extractall(path)
    
    #set path to current year
    os.chdir('{0}\\data\\{1}\\'.format(cdir, year))
    
    #Astros joined AL in 2013
    #Marlins changed name from FLA to MIA in 2012
    for team, league in list(zip(teams,leagues)):
        if not (team == 'HOU' and year<=2012 and league=='EVA') and not (team == 'HOU' and year>=2013 and league=='EVN') and not (team == 'FLO' and year>=2012) and not (team == 'MIA' and year<=2011):
            os.system('BEVENT -y {0} -f 0-96 {0}{1}.{2} > {0}{1}.BEV'.format(year,team,league))
            
    #remove unnecessary file types
    for extension in ['EVA','EVN','ROS']:
        os.system('del /S *.{0}'.format(extension))
