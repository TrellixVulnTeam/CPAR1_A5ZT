
import itertools
import sys
import numpy as np
import pandas as pd
from dbconnect import dbconnect

connector = dbconnect.DatabaseConnect('CHECK_CPAR2')

_pre_post_month_window = sys.argv[0]
_release_num = sys.argv[1]
_preg_age_cutoff = 15
to_sql = True

pat_query = """SELECT * FROM rid_pre_post_pat_windows where negative_duration >= {}
                and positive_duration >= {} AND ReleaseNum = {}""".format(_pre_post_month_window,
                                                                         _pre_post_month_window,
                                                                         _release_num)

pt_df = connector.query(pat_query,parse_dates=['Program_Date'])

demo_df = connector.query("""
SELECT RecipientID, Asthma, Diabetes, SCD, Prematurity,
Brain_Injury, Epilepsy, Diagnosis_Category as Diagnosis,
Enrollment_Risk as Risk, Enrollment_Age as Age, Enrollment_Age_Category as Age_Cat,
Gender, DOB as Birth_Date,Preg_Flag as Pregnancy,
E2,E4,HC,HE2,HE4 FROM pat_info_complete""")

pat_info = pd.merge(pt_df,demo_df,on='RecipientID',how='inner')

pat_info['Max_Date'] = pat_info['Program_Date'].apply(lambda x: x + pd.DateOffset(months=_pre_post_month_window))
pat_info['Min_Date'] = pat_info['Program_Date'].apply(lambda x: x - pd.DateOffset(months=_pre_post_month_window))
pat_info['ReleaseNum'] = _release_num
pat_info['Duration'] = _pre_post_month_window

#adds all patients that meet criteria into patient_info
cols = ['RecipientID','Duration','Program_Date','Asthma','Diabetes','SCD',
        'Prematurity','Brain_Injury','Epilepsy','Diagnosis','Risk','Age_Cat','Age','Gender',
        'E2','E4','HC','HE2','HE4','Pregnancy','ReleaseNum']

if to_sql == True:
    connector.insert(pat_info[cols],'rid_pre_post_pat_info')

# for faster claims query selects RecipientIDs
unique_recipientID = list(pat_info['RecipientID'].unique())
unique_recipientID = unique_recipientID[:-1]
unique_recipientID = ",".join(unique_recipientID)

mcn_claims_query = """SELECT
    RecipientID,
    DCN,
    ServiceFromDt,
    ServiceThruDt,
    CatgofServiceCd,
    Category1,
    Category2,
    Category3,
    CHECK_Category,
    SUM(AdjustedPriceAmt) as AdjustedPriceAmt,
    visit as Visit,
    serviceCount as ServiceCount,
    procedureCount as ProcedureCount,
    encounter Encounter,
    VisitInpatientDays
FROM
    CHECK_CPAR.tsc_hfs_main_claims_new
WHERE RecipientID in ({}) and Prematurity_Ineligible = 0
GROUP BY RecipientID , DCN, CHECK_Category, ServiceFromDt
order by RecipientID, DCN, ServiceLineNbr """.format(unique_recipientID)

pharmacy_claims_query = """SELECT
    RecipientID,
    DCN,
    ServiceFromDt,
    ServiceFromDt as ServiceThruDt,
    CatgofServiceCd,
    'Pharmacy' as Category1,
    'Pharmacy' as Category2,
    'Pharmacy' as Category3,
    'Pharmacy' as CHECK_Category,
    AdjustedPriceAmt,
    0 as Visit,
    0 as ServiceCount,
    0 as ProcedureCount,
    0 as Encounter,
    0 as VisitInpatientDays
FROM
    CHECK_CPAR.tsc_hfs_pharmacy
WHERE RecipientID in ({})
GROUP BY RecipientID, DCN, ServiceFromDt
order by RecipientID, DCN, ServiceLineNbr  """.format(unique_recipientID)

mcn_df = connector.query(mcn_claims_query)
pharmacy_df = connector.query(pharmacy_claims_query)
tot_cost_df = pd.concat([mcn_df,pharmacy_df])

cost_pt_info_df = pd.merge(pat_info[['RecipientID','Program_Date','Min_Date','Max_Date','Duration']],tot_cost_df,how='left')

cost_pt_info_df['ServiceFromDt'] = pd.to_datetime(cost_pt_info_df['ServiceFromDt'])

# this will remove patients with no bills
cost_pt_info_df = cost_pt_info_df.loc[cost_pt_info_df['ServiceFromDt'].between(cost_pt_info_df['Min_Date'],
                                                                               cost_pt_info_df['Max_Date'])]

cost_pt_info_df['Pre_Post'] = cost_pt_info_df['Program_Date']>cost_pt_info_df['ServiceFromDt']
cost_pt_info_df['Pre_Post'] = cost_pt_info_df['Pre_Post'].replace({True:'Pre',False:'Post'})

# selects patients that have no bills will be appended with 0's on pivot
pts_no_bills = pat_info.loc[~pat_info['RecipientID'].isin(cost_pt_info_df['RecipientID']),
                           ['RecipientID','Program_Date','Duration']]

cost_pt_info_df['CHECK_Category'] = cost_pt_info_df['CHECK_Category'].replace({'OTHER':'Other',
                                                                               'INPATIENT':'Inpatient',
                                                                               'OUTPATIENT':'Outpatient'})

pre_post_pivot = pd.pivot_table(cost_pt_info_df,index=['RecipientID','Duration'],
                                columns=['Pre_Post','CHECK_Category'],
                                values=['AdjustedPriceAmt','Visit','VisitInpatientDays'],aggfunc=np.sum,fill_value=0)

pre_post_pivot.reset_index(inplace=True)

pre_post_dfs = {'AdjustedPriceAmt':None,'Visit':None,'VisitInpatientDays':None}

for i in pre_post_dfs.keys():

    pre_post_pivot = pd.pivot_table(cost_pt_info_df,index=['RecipientID','Program_Date'],
                                    columns=['Pre_Post','CHECK_Category'],
                                    values=i,aggfunc=np.sum,fill_value=0)

    pre_post_pivot.columns =  [col[1]+"_"+col[0] for col in pre_post_pivot.columns.values]
    pre_post_pivot.reset_index(inplace=True)
    # where patients with no bills are added on
    pre_post_pivot = pd.concat([pre_post_pivot,pts_no_bills])
    pre_post_pivot = pre_post_pivot.fillna(0)

    pre_cols = ['ED_Pre', 'Inpatient_Pre', 'Other_Pre', 'Outpatient_Pre', 'Pharmacy_Pre']
    post_cols = ['ED_Post', 'Inpatient_Post', 'Other_Post', 'Outpatient_Post', 'Pharmacy_Post']

    pre_post_pivot['Total_Pre'] = pre_post_pivot[pre_cols].sum(axis=1)
    pre_post_pivot['Total_Post'] = pre_post_pivot[post_cols].sum(axis=1)
    pre_post_pivot['Duration'] = _pre_post_month_window
    pre_post_pivot['Aggregation_Type'] = i
    pre_post_pivot['ReleaseNum'] = _release_num

    individual_columns = ['RecipientID','Duration','Aggregation_Type']
    cost_columns = ['Inpatient_Pre','Inpatient_Post','Outpatient_Pre',
                    'Outpatient_Post','ED_Pre','ED_Post','Other_Pre','Other_Post',
                    'Pharmacy_Pre','Pharmacy_Post','Total_Pre','Total_Post','ReleaseNum']

    if to_sql == True:
        connector.insert(pre_post_pivot[individual_columns+cost_columns],'rid_pre_post_individual')

    #prepare dataframe for later grouping
    pre_post_dfs[i] = pd.merge(pre_post_pivot,pat_info,
                           on=['RecipientID','Program_Date','ReleaseNum','Duration'],
                           how='left')

agg_func = np.mean
agg_values = {i:agg_func for i in cost_columns}
agg_values['RecipientID'] = np.count_nonzero

to_sql = True
cols_to_agg = [['Holistic'],['Diagnosis'],['Age_Cat'],['Diagnosis','Age_Cat'],
               ['Risk'],['Risk','Diagnosis'],['Gender','Diagnosis'],['Gender','Pregnancy']]
df_to_group = ['AdjustedPriceAmt','Visit','VisitInpatientDays']


for value in df_to_group:
    temp = pre_post_dfs[value].copy()
    temp['Holistic'] = "1"
    temp.loc[temp['E2']==1,'Population_Type']  = 'E2'
    temp.loc[temp['E4']==1,'Population_Type']  = 'E4'
    temp.loc[temp['HE2']==1,'Population_Type']  = 'HE2'
    temp.loc[temp['HE4']==1,'Population_Type']  = 'HE4'
    temp.loc[temp['HC']==1,'Population_Type']  = 'HC'
    temp['Pregnancy'] = temp['Pregnancy'].map({1:'Pos_Preg',0:'Neg_Preg'})

    for x in cols_to_agg:
        agg_col = ['Population_Type']
        agg_col += x
        group_index = []
        for i in agg_col:
            group_index.append(temp[i].unique())
        # this makes sure there is a complete index even when the count of a group is equal to 0
        group_index = itertools.product(*group_index)
        total_group_combinations = pd.DataFrame(index=group_index)
        group_df = temp.groupby(agg_col).agg(agg_values)
        group_df['Group_Subset']= group_df.index.get_values()

        group_df.rename_axis({'RecipientID':'N'},axis=1,inplace=True)
        group_df = pd.merge(total_group_combinations,group_df,left_index=True,right_on='Group_Subset',how='left')
        group_df['Group_Type'] = "::".join(agg_col)
        group_df['Duration'] = _pre_post_month_window
        group_df['Aggregation_Function'] = agg_func.__name__
        group_df['Value'] = value
        group_df['ReleaseNum'] = _release_num
        group_df[cost_columns] = group_df[cost_columns].round(2)
        group_df['Group_Subset'] = group_df['Group_Subset'].apply(lambda x: "::".join(x))
        group_df = group_df.fillna(0)
        group_df = group_df.reset_index(drop=True)
        group_df = group_df[['Group_Type','Group_Subset','Aggregation_Function',
                           'Value','N','Duration']+cost_columns]
        if to_sql == True:
            connector.insert(group_df,'rid_pre_post_groupings')
