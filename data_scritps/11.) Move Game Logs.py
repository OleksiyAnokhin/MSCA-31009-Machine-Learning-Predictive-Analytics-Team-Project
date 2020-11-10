# -*- coding: utf-8 -*-
"""
Created on Sun Nov  8 11:50:33 2020

@author: mthom
"""


import os

cdir = os.getcwd()
for yr in range(2006, 2020):
    #move zip file to directory
    os.system('move .\\data\\gl{0}.zip .\\data\\{0}\\gl{0}.zip'.format(yr))
    
