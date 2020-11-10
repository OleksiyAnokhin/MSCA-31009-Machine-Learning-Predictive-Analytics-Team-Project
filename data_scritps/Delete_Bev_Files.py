# -*- coding: utf-8 -*-
"""
Created on Mon Nov  2 16:57:31 2020

@author: mthom
"""



import os

cdir = os.getcwd()

for i in range(2006, 2020):    
    os.chdir('{0}\\data\\{1}\\'.format(cdir, i))
    os.system('del /S *.BEV')
    os.system('del TEAM{}'.format(i))