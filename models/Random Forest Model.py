# -*- coding: utf-8 -*-
"""
Created on Tue Nov 24 10:54:08 2020

@author: mthom
"""


import pandas as pd
from sklearn.ensemble import RandomForestRegressor
import numpy as np

df = pd.read_pickle('./ml_data.pkl')

all_features = ['PA_avg', 'AB_avg', 'SAC_avg', 'HBP_avg', 'BB_avg', 'S_avg', 
        'D_avg', 'T_avg', 'HR_avg', 'H_avg', 'TB_avg', 'SO_avg', 'RBI_avg', 'R_avg', 'CS_avg', 'SB_avg',
       'OBP_avg', 'SLG_avg', 'OPS_avg', 'OPS_Adj_avg', 'next_home', 'next_park_factor_SLG', 'next_park_factor_OBP',
       'pts_500', 'park_ratio_OBP', 'park_ratio_SLG', 'pts_500_parkadj',
       'SLG_avg_parkadj', 'OPS_avg_parkadj', 'whip_SO_B', 'whip_SO_B_parkadj',
       'IP_avg_p', 'PA_avg_p', 'AB_avg_p', 'SAC_avg_p', 'HBP_avg_p',
       'BB_avg_p', 'S_avg_p', 'D_avg_p', 'T_avg_p', 'HR_avg_p', 'H_avg_p',
       'TB_avg_p', 'SO_avg_p', 'RBI_avg_p', 'R_avg_p', 'CS_avg_p', 'SB_avg_p',
       'pts_500_p', 'OBP_avg_p', 'SLG_avg_p', 'OPS_avg_p', 'OPS_Adj_avg_p',
       'pts_500_parkadj_p', 'SLG_avg_parkadj_p', 'OPS_avg_parkadj_p',
       'whip_SO_parkadj_p', 'first_time_pitcher', 'order', 'ops x pts_500 order15',
       'ops x pts_500 parkadj', 'ops23 x pts_500', 'ops x pts_500 orderadj']


features = ['pts_500', 'pts_500_p', 'OBP_avg', 'SLG_avg', 'SB_avg', 'RBI_avg', 'R_avg', 'home', 
            'first_time_pitcher', 'park_ratio_OBP', 'park_ratio_SLG','order',  
            'SO_avg_p', 'pts_500_parkadj_p', 'pts_500_parkadj','SLG_avg_parkadj',
            'OPS_avg_parkadj','SLG_avg_parkadj_p',
            'OPS_avg_parkadj_p','pts_BxP','SLG_BxP','OPS_BxP','whip_SO_BxP','whip_SO_B','whip_SO_B_parkadj',
            'order', 'ops x pts_500 order15', 'ops x pts_500 parkadj', 'ops23 x pts_500', 'ops x pts_500 orderadj',
            'whip_p', 'whip_SO_p', 'whip_SO_parkadj_p', 'whip_parkadj_p']


X_train = df[features][(df['year']<2017)&(df['year']>=2010)]
X_test = df[features][df['year']>=2017]

y_train = df['pts'][(df['year']<2017)&(df['year']>=2010)]
y_test = df['pts'][df['year']>=2017]


rand_index = np.random.rand(len(X_train)).argsort(axis=0)[0:20000]
X_train_subset = X_train.iloc[rand_index]
y_train_subset = y_train.iloc[rand_index]

RF = RandomForestRegressor(n_jobs=-1, random_state=42, max_depth=5, n_estimators=500)
RF.fit(X_train_subset, y_train_subset)

y_pred = RF.predict(X_test)

pred = pd.DataFrame({'pred':y_pred,'actual':y_test,'year':df['year'][df['year']>=2017]})

pred['residuals'] = pred['actual'] - pred['pred']
pred['MAE'] = pred['residuals'].abs()
pred['RMSE'] = pred['residuals']**2

cor = pred[['actual','pred','year']].groupby('year').corr().reset_index()
cor = cor[cor['level_1']=='actual'][['year','pred']]
cor.set_index('year', inplace=True)
cor.columns = ['correlation']

cor.loc['Total','correlation'] =  pred[['actual','pred']].corr().iloc[0,1]


err = pred[['year','MAE','RMSE']].groupby('year').mean()
err['RMSE'] = err['RMSE']**0.5
err.loc['Total', 'MAE'] = pred['residuals'].abs().mean()

err.loc['Total', 'RMSE'] = pred['RMSE'].mean()**0.5
cor = cor.join(err)


print(cor)



