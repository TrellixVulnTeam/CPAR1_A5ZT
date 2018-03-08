import pandas as pd
import numpy as np
from CHECK.dbconnect import dbconnect

def mc_pivot(mc_df, columns, values, margins_name):
    '''pivots data to get sums of column values and renames heirrchial columns'''
    pivot_mc_df = pd.pivot_table(mc_df,index=['RecipientID'],columns=columns, values=values,
                                 fill_value=0,aggfunc=np.sum, margins=True, margins_name=margins_name)
    pivot_mc_df.columns = pivot_mc_df.columns = [col[1]+"_"+col[0] for col in pivot_mc_df.columns.values]
    pivot_mc_df.reset_index(inplace=True)
    pivot_mc_df = pivot_mc_df[:-1]
    return pivot_mc_df

connector = dbconnect.DatabaseConnect('CHECK_CPAR2')
release_info  = connector.query("""Select ReleaseNum, HFS_Release_Date FROM hfs_release_info
WHERE ReleaseNum = (SELECT MAX(ReleaseNum) from hfs_release_info)""")
#subtracts 1 to get bills since last release
relnum = release_info['ReleaseNum'][0] - 1
reldate = release_info['HFS_Release_Date'][0].strftime('%Y-%m-%d')

min_window_dt = connector.query('''Select max(servicefromdt) as min_ser_dt from
                                   tsc_hfs_main_claims where ReleaseNum = {};'''.format(relnum))
min_window_dt = min_window_dt['min_ser_dt'][0].strftime('%Y-%m-%d')

mc_df = connector.query('''Select RecipientID, CHECK_Category, sum(visit) as Visit, count(*) as Encounters,
min(ServiceFromDt) as 'Min_ServiceFromDt', Max(ServiceFromDt) as 'Max_ServiceFromDt',
sum(AdjustedPriceAmt) as AdjustedPriceAmt from tsc_hfs_main_claims_new where
ServiceFromDt > '{}' and ServiceFromDt <= '{}' group by RecipientID, CHECK_Category;'''.format(min_win_dt,reldate)
                                                    ,parse_dates = ['Max_ServiceFromDt','Min_ServiceFromDt'])

high_util_df = mc_pivot(mc_df,'CHECK_Category',['Visit','Encounters','AdjustedPriceAmt'],'Release')
high_util_df = high_util_df[['RecipientID','ED_Visit','INPATIENT_Visit','OTHER_Visit','OUTPATIENT_Visit',
                             'Release_Visit','Release_Encounters','Release_AdjustedPriceAmt']]

mc_min_max_service_dts = mc_df.groupby(['RecipientID'],as_index=False).agg({'Min_ServiceFromDt':np.min,
                                                                            'Max_ServiceFromDt':np.max})
high_util_df = pd.merge(high_util_df, mc_min_max_service_dts, on='RecipientID', how='inner')
high_util_df['Total_Duration_Days'] = (((high_util_df['Max_ServiceFromDt'] - high_util_df['Min_ServiceFromDt'])
                                       / np.timedelta64(1, 'D')).astype(int)) + 1

unique_rin_str = ",".join(mc_df['RecipientID'].unique())

cum_mc_df = connector.query("""Select a.RecipientID,sum(a.AdjustedPriceAmt) as AdjustedPriceAmt, a.CHECK_Category,
sum(a.visit) as Visit, count(*) as Encounters from tsc_hfs_main_claims_new a, pat_info_complete b
where a.RecipientID = b.RecipientID and a.ServiceFromDt >= b.Program_Date and a.ServiceFromDt <= '{}'
and a.RecipientID in ({}) group by a.RecipientID, a.CHECK_Category;""".format(reldate, unique_rin_str))

pat_info = connector.query("""SELECT RecipientID, PatientID, MRN, Asthma, Diabetes, SCD, Prematurity, Brain_Injury,
Epilepsy, Program_Date, Current_Risk, HC, E2, E4, HE2, HE4 from pat_info_complete where RecipientID in ({})""".format(unique_rin_str),parse_dates=['Program_Date'])

cum_high_util_df = mc_pivot(cum_mc_df,'CHECK_Category',['Visit','Encounters','AdjustedPriceAmt'],'Cumulative')

cum_high_util_df = cum_high_util_df[['RecipientID','Cumulative_Visit','Cumulative_Encounters','Cumulative_AdjustedPriceAmt']]
high_util_df = pd.merge(high_util_df, cum_high_util_df,on='RecipientID')
high_util_df = pd.merge(high_util_df, pat_info,on='RecipientID',how='left')
high_util_df['Months_In_Program'] = ((pd.to_datetime(reldate) -
                                     high_util_df['Program_Date']) / np.timedelta64(1, 'M')).astype(int)
#increments to current
high_util_df['Release_Number'] = int(relnum) + 1

# high_util_df = high_util_df[['RecipientID','MRN','PatientID','Asthma','Diabetes','SCD','Prematurity','Epilepsy',
#                              'Brain_Injury','Min_ServiceFromDt','Max_ServiceFromDt','Total_Duration_Days',
#                              'Total_AdjustedPriceAmt','Total_Visit','Total_Encounters','ED_Visit',
#                              'INPATIENT_Visit','OTHER_Visit','OUTPATIENT_Visit','Cumulative_AdjustedPriceAmt',
#                              'Cumulative_Visits','Cumulative_Encounters','Current_Risk','Program_Date',
#                              'Months_in_program','E2','E4','HC','HE4','HE2','Release_Number']]

# connector.insert(high_util_df,'rpt_high_utilizers')
