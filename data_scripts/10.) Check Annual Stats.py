# -*- coding: utf-8 -*-
"""
Created on Sun Nov  8 10:39:38 2020

@author: mthom
"""


from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re
import requests
from sqlalchemy import create_engine
from password import password

#connect to site
url = 'https://www.baseball-reference.com/leagues/MLB/bat.shtml'
headers = {'User-Agent': "Chrome/54.0.2840.90"}
response = requests.get(url, headers=headers)
html = response.content 

#find totals table
soup = BeautifulSoup(html, 'html.parser')    
div = soup.find('div', attrs={'id':"all_teams_standard_batting_totals"})
td = div.find_all('td')
cols = [x.attrs['data-stat'] for x in td[0:27]]

#get stats
data = np.array([x.text for x in td])
data = data.reshape(-1,27)
df = pd.DataFrame(data)
df.columns = cols

#add year
yr = div.find_all('a', attrs={'href':re.compile("/leagues/MLB/")})
yrs = [x.string for x in yr]
df.insert(0, 'year', pd.DataFrame(yrs, columns=['year']))
df['year'] = df['year'].astype(int)
df = df[df['year']>=2006]

#aggregate at bat data
pw = password()
con = create_engine("mysql+mysqlconnector://{0}:{1}@{2}/mlb_db".format(pw.user, pw.password, pw.host))
SQL = 'SELECT Year(START) as year, SUM(R) as R, SUM(CS) as CS, SUM(RBI) as RBI, SUM(SB) as SB, SUM(H) as H, sum(D) as 2B, sum(T) as 3B, sum(HR) as HR FROM at_bats GROUP BY YEAR(START)' 
stats = pd.read_sql_query(SQL, con)    

#compare our data to Baseball Reference.com
stats = stats.merge(df[['year', 'R', 'CS', 'RBI', 'SB', 'H', '2B', '3B', 'HR']], 
                    how='left', on=['year'], suffixes=['','_baseball_reference'])

for col in [col for col in stats.columns if '_ref' in col]:
    stats[col] = stats[col].astype(int)

for col in ['R', 'CS', 'RBI', 'SB', 'H', '2B', '3B', 'HR']:
    stats['diff_{}'.format(col)] = stats[col] - stats['{}_baseball_reference'.format(col)]
    
cols = ['year'] + [col for col in stats.columns if 'diff' in col]

print('differences in total stats by year:\n')
print(stats[cols])    
