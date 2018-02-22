
import pymysql
import pandas as pd
import numpy as np
from datetime import timedelta, date
from dbconnect import dbconnect


connector = dbconnect.DatabaseConnect('CHECK_CPAR2')
release_num = input("Enter in release num: ")
max_date = connector.query('''SELECT HFS_Release_Date FROM release_info where ReleaseNum = {}'''.format(release_num))
max_date = max_date.values[0][0]

ip_ed_df = connector.query('''
SELECT
    RecipientID,ServiceFromDt,
    'IP' AS Category,COUNT(1) AS encounters
FROM
    tsc_hfs_main_claims_new
WHERE
    Category3 IN ('INPATIENT_IP' , 'EMERGENCY_IP')
        AND RejectionStatusCd = 'N'
GROUP BY RecipientID , ServiceFromDt
UNION SELECT
    RecipientID,ServiceFromDt,
    'ED' AS Category,COUNT(1) AS encounters
FROM
    tsc_hfs_main_claims_new
WHERE
    Category3 IN ('EMERGENCY_OP')
        AND RejectionStatusCd = 'N'
GROUP BY RecipientID , ServiceFromDt;''')

ip_ed_df = pd.pivot_table(ip_ed_df,index=['RecipientID','ServiceFromDt'],columns='Category',
                          values='encounters',aggfunc='first',fill_value=0)
ip_ed_df.reset_index(inplace=True)

enroll_df = connector.query('''select RecipientID, Initial_Enrollment_Date from pat_info_demo;''')
engage_df = connector.query('''select RecipientID, Engagement_Date from pat_info_demo WHERE
Engagement_Date is not null;''')

enroll_df['Current_Date'] = max_date

def risk_tier_calc(pat_df_in,risk_col_name):
    '''calculates risk for 12 the months from '''
    pat_df_in.loc[(pat_df_in['ED']>3)|(pat_df_in['IP']>1),risk_col_name] = 'High'
    pat_df_in.loc[((pat_df_in['ED']<=3)&(pat_df_in['ED']>=1))|
                  (pat_df_in['IP']==1),risk_col_name] = 'Medium'
    pat_df_in.loc[(pat_df_in['ED']==0)&(pat_df_in['IP']==0),risk_col_name] = 'Low'
    return pat_df_in

def risk_window(pat_df_in,ed_ip_df,date_col,risk_type):
    '''Selects IP and ED bills that occured between the date_col and 12 months back
    pat_df_in: pandas dataframe w/ columns ED, IP, ServiceFromDt, and date_col
    ed_ip_df: pandas dataframe'''
    window_size = 12
    pat_df = pat_df_in.copy()
    pat_df['Low_Window_Date'] = pat_df[date_col].apply(lambda x: x - pd.DateOffset(months=window_size))
    pat_df = pd.merge(pat_df, ed_ip_df, on = 'RecipientID', how = 'left')
    pat_df['ServiceFromDt'] = pd.to_datetime(pat_df['ServiceFromDt'])
    pat_df[date_col] = pd.to_datetime(pat_df[date_col])
    pat_df = pat_df.loc[pat_df['ServiceFromDt'].between(pat_df['Low_Window_Date'],pat_df[date_col])]
    pat_df = pat_df.groupby(['RecipientID'],as_index=False).agg({'ED':np.sum,'IP':np.sum})
    pat_df = pd.merge(pat_df_in[['RecipientID']],pat_df,on='RecipientID',how='left')
    pat_df.fillna(0,inplace=True)
    pat_df = risk_tier_calc(pat_df,risk_type)
    pat_df.set_index('RecipientID',inplace=True)
    return pat_df

enrollment_risk_df = risk_window(enroll_df,ip_ed_df,'Initial_Enrollment_Date','Enrollment_Risk')
current_risk_df = risk_window(enroll_df,ip_ed_df,'Current_Date','Current_Risk')
engagement_risk_df = risk_window(engage_df,ip_ed_df,'Engagement_Date','Engagement_Risk')

risk_cols = ['Enrollment_Risk','Engagement_Risk','Current_Risk','UCSD_Risk_Raw','UCSD_Risk']
total_risk_df = pd.DataFrame(index=enrollment_risk_df.index,columns=risk_cols)

total_risk_df.loc[engagement_risk_df.index,
                  'Engagement_Risk'] = engagement_risk_df['Engagement_Risk']
total_risk_df.loc[current_risk_df.index,
                  'Current_Risk'] = current_risk_df['Current_Risk']
total_risk_df.loc[enrollment_risk_df.index,
                  'Enrollment_Risk'] = enrollment_risk_df['Enrollment_Risk']

total_risk_df.reset_index(inplace=True)
connector.replace(total_risk_df,'pat_info_risk')
