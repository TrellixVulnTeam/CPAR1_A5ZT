import pandas as pd
import sys
import configparser
import itertools
import numpy as np
from dbconnect import dbconnect

connector = dbconnect.DatabaseConnect('CHECK_CPAR2')

relnum = sys.argv[0]
reldate = sys.argv[1]

def pivottable(df2, column1, column2):
    df1 =  df2.pivot(index='RecipientID', columns=column1, values=column2,fill_value=0)
    df1.reset_index(inplace=True)
    df1['Total_'+column2] = df1['ED','INPATIENT','OUTPATIENT','OTHER'].sum()
    return df1

def mapcolumns(df1, df2, col1):
    df2[col1] = df2['RecipientID'].map(df1.set_index('RecipientID')[col1])
    return df2

min_win_dt = connector.query('''Select max(servicefromdt) as min_ser_dt from
                                tsc_hfs_main_claims where ReleaseNum = {};'''.format(relnum),
                                parse_dates=['min_ser_dt'])
min_win_dt = min_win_dt['min_ser_dt'][0].strftime('%Y-%m-%d')

mc_df = connector.query('''Select RecipientID, CHECK_Category, sum(visit) as Visit, count(*) as Encounters,
min(ServiceFromDt) as 'Min_ServiceFromDt', Max(ServiceFromDt) as 'Max_ServiceFromDt',
sum(AdjustedPriceAmt) as AdjustedPriceAmt from tsc_hfs_main_claims_new where ServiceFromDt > '{}'
and ServiceFromDt <= '{}' group by RecipientID, CHECK_Category;'''.format(min_win_dt,reldate))

high_util_df = pivottable(mc_df,'CHECK_Category','Visit')
high_util_df1 = pivottable(mc_df,'CHECK_Category','Encounters')

high_util_df = mapcolumns(high_util_df1,high_util_df,'Total_Encounters')
high_util_df2 = pivottable(mc_df,'CHECK_Category','AdjustedPriceAmt')

high_util_df = mapcolumns(high_util_df2,high_util_df,'Total_AdjustedPriceAmt')

mc_min_max_service_dts = mc_df.groupby(['RecipientID'],as_index=False).agg({'Min_ServiceFromDt':np.min,
                                                                            'Max_ServiceFromDt':np.max})
high_util_df = pd.merge(high_util_df, mc_min_max_service_dts,on='RecipientID',how='inner')

high_util_df['Min_ServiceFromDt'] = pd.to_datetime(high_util_df['Min_ServiceFromDt'])
high_util_df['Max_ServiceFromDt'] = pd.to_datetime(high_util_df['Max_ServiceFromDt'])

high_util_df['Total_Duration_Days'] = high_util_df['Max_ServiceFromDt'] - high_util_df['Min_ServiceFromDt']
high_util_df['Total_Duration_Days'] = (high_util_df['Total_Duration_Days'] / np.timedelta64(1, 'D')).astype(int)
high_util_df['Total_Duration_Days'] = high_util_df['Total_Duration_Days'] + 1

cum_mc_df = connector.query("""Select a.RecipientID,b.MRN,b.PatientID, sum(a.AdjustedPriceAmt) as CumAdjustedPriceAmt, a.CHECK_Category,
sum(a.visit) as CumVisits, count(*) as CumEncounters, b.Asthma, b.Diabetes,
b.SCD, b.Prematurity, b.Brain_Injury, b.Epilepsy, b.Program_Date,b.Current_Risk, b.HC, b.E2, b.E4, b.HE2,b.HE4
from tsc_hfs_main_claims_new a,pat_info_complete b where a.RecipientID = b.RecipientID and a.ServiceFromDt >= b.Program_Date and a.ServiceFromDt <= '{}'
group by a.RecipientID, a.CHECK_Category;""".format(reldate))

cumvisit_df = pivottable(cum_mc_df, 'CHECK_Category','CumVisits')
cum_enc_df = pivottable(cum_mc_df, 'CHECK_Category','CumEncounters')
cum_adjprice_df = pivottable(cum_mc_df, 'CHECK_Category','CumAdjustedPriceAmt')

high_util_df = pd.merge(high_util_df, cumvisit_df[['RecipientID','Total_CumVisits']],on='RecipientID')
high_util_df = pd.merge(high_util_df, cum_enc_df[['RecipientID','Total_CumEncounters']],on='RecipientID')
high_util_df = pd.merge(high_util_df, cum_adjprice_df[['RecipientID','Total_CumAdjustedPriceAmt']],on='RecipientID')

high_util_df = pd.merge(high_util_df, cum_mc_df[['RecipientID','MRN','PatientID','Asthma','Diabetes','SCD','Prematurity','Epilepsy','Brain_Injury','Program_Date','Current_Risk','E2','E4','HC','HE4','HE2']].drop_duplicates(),on='RecipientID',how='left')

high_util_df['Months_in_program'] = (pd.to_datetime('{}'.format(reldate),format='%Y-%m-%d') - pd.to_datetime(high_util_df['Program_Date']))/ np.timedelta64(1, 'M')
high_util_df['Months_in_program'] = high_util_df['Months_in_program'].astype(int)
high_util_df['Release_Number'] = int(relnum) + 1

high_util_df = high_util_df[['RecipientID','MRN','PatientID','Asthma','Diabetes','SCD','Prematurity','Epilepsy','Brain_Injury','Min_ServiceFromDt','Max_ServiceFromDt',
                             'Total_Duration_Days','Total_AdjustedPriceAmt','Total_Visit','Total_Encounters','INPATIENT','OUTPATIENT','ED','OTHER','Total_CumAdjustedPriceAmt',
                             'Total_CumVisits','Total_CumEncounters','Current_Risk','Program_Date','Months_in_program','E2','E4','HC','HE4','HE2','Release_Number']]

connector.insert(high_util_df,'rpt_high_utilizers')
