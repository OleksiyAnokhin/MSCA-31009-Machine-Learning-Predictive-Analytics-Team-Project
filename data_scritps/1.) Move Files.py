# -*- coding: utf-8 -*-
"""
Created on Mon Nov  1 14:03:17 2020

@author: mthom
"""

import os

cdir = os.getcwd()
for yr in range(2006, 2020):
    
    #create directory
    os.system('mkdir .\\data\\{0}'.format(yr))
    #move zip file to directory
    os.system('move .\\data\\{0}eve.zip .\\data\\{0}\\{0}eve.zip'.format(yr))
    #copy BEVENT.EXE into directory
    os.system('copy .\\data\\BEVENT.EXE .\\data\\{0}\\BEVENT.EXE'.format(yr))
