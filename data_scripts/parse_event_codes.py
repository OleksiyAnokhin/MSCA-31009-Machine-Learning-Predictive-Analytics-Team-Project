# -*- coding: utf-8 -*-
"""
Created on Mon Nov  2 15:59:24 2020

@author: mthom
"""


def plate_appearance(x):
    if at_bat(x) or walks(x) or hbp(x) or sac(x): 
        return 1
    else:
        return 0

def at_bat(x):
    if hit(x) or out(x):
        return 1
    else:
        return 0

def hbp(x):
    if x[0:2]=='HP':
        return 1
    else:
        return 0

#note the POCSH and CSH ([Pick-off] caught stealing home are not sacrifice hits [SH])
def sac(x):
    try:
        int(x[0])
        if 'SF' in x or 'SH' in x:
            return 1
        return 0
    except ValueError:
        if (x[0:2] == 'FC' or x[0] == 'E') and ('SF' in x or 'SH' in x):
            return 1
        else:
            return 0

def out(x):
    try:
        int(x[0])
        if 'SF' in x or 'SH' in x:
            return 0
        return 1
    except ValueError:
        if ((x[0:2] == 'FC' or x[0] == 'E') and 'SF' not in x and 'SH' not in x) or x[0]=='K':
            return 1
        else:
            return 0
    
def strikeout(x):
    if x[0] == 'K':
        return 1
    else:
        return 0

def walks(x):
    #Note that WP is a wild pitch
    if (x[0] =='W'): 
        if len(x) == 1:
            return 1
        else:
            if x[1] == 'P':
                return 0
            else:
                return 1
    else:
        if x[0:2]=='IW':
            return 1
        return 0
    
def single(x):
    if (x[0] =='S' and x[1] != 'B'): #SB is stolen base
        return 1
    else:
        return 0

def double(x):
    if (x[0]=='D' and (x[1] != 'I' or x[1:3]=='GR')): #GR is ground rule double; DI is defensive indifference (not counted as a stolen base)
        return 1
    else:
        return 0

def triple(x):
    if (x[0]=='T'):
        return 1
    else:
        return 0
   
def home_run(x):
    if x[0:2]=='HR':
        return 1
    else:
        return 0

def hit(x):
    if (x[0] in ['S', 'D', 'T'] and x[1] not in ['B', 'I']) or x[0:2]=='HR' or x[0:3]=='DGR':
        return 1
    else:
        return 0
