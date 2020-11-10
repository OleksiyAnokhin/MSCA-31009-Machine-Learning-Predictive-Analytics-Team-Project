# -*- coding: utf-8 -*-
"""
Created on Mon Nov  9 18:58:56 2020

@author: mthom
"""

import pandas as pd
from sqlalchemy import create_engine
from password import password

pw = password()
engine = create_engine('mysql+mysqlconnector://{}:{}@{}:3306/mlb_db'.format(pw.user, pw.password, pw.host), echo=False)

file = '_events_with_sb'
for yr in range(2006, 2020):
    df = pd.read_pickle('.\\data\\{0}\\{0}{1}.pkl'.format(yr, file))
    df.drop('Home', inplace=True, axis=1)
    df.to_sql(con=engine, name='at_bats', if_exists='append', chunksize=10000, index=False)
    print(yr)