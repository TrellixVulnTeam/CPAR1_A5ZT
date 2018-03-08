
import itertools
import numpy as np
import pandas as pd
from CHECK.dbconnect import dbconnect
from CHECK.conconnect import conconnect

def pp_total_cost_query(unique_recipientID, connector):
    '''unique_recipientID: pd.Series of RecipientIDs
    Returns all bills for patients that have them returns all bills for the unique_recipientIDs'''
    unique_recipientID_list = ",".join(unique_recipientID)

    mcn_claims_query = """SELECT
        RecipientID,DCN,ServiceFromDt,
        ServiceThruDt,CHECK_Category,
        SUM(AdjustedPriceAmt) as AdjustedPriceAmt,
        visit as Visit,serviceCount as ServiceCount,
        procedureCount as ProcedureCount,encounter Encounter,
        VisitInpatientDays
    FROM
        CHECK_CPAR.tsc_hfs_main_claims_new
    WHERE RecipientID in ({}) and Prematurity_Ineligible = 0
    GROUP BY RecipientID , DCN, CHECK_Category, ServiceFromDt
    order by RecipientID, DCN, ServiceLineNbr """.format(unique_recipientID_list)

    pharmacy_claims_query = """SELECT
        RecipientID,DCN,ServiceFromDt,ServiceFromDt as ServiceThruDt,
        'Pharmacy' as CHECK_Category,AdjustedPriceAmt,0 as Visit,
        0 as ServiceCount,0 as ProcedureCount,0 as Encounter,
        0 as VisitInpatientDays
    FROM
        CHECK_CPAR.tsc_hfs_pharmacy
    WHERE RecipientID in ({})
    GROUP BY RecipientID, DCN, ServiceFromDt
    order by RecipientID, DCN, ServiceLineNbr  """.format(unique_recipientID_list)

    mcn_df = connector.query(mcn_claims_query,parse_dates=['ServiceFromDt','ServiceThruDt'])
    pharmacy_df = connector.query(pharmacy_claims_query,parse_dates=['ServiceFromDt','ServiceThruDt'])
    tot_cost_df = pd.concat([mcn_df,pharmacy_df])
    return tot_cost_df


def pp_bill_filter(pat_info,pt_w_costs,unique_pt_list):
    '''
    pat_info: pd.DataFrame
    pt_w_costs_df: pd.DataFrame with columns
    pt_wo_costs: pd.Series of patients that had no costs
    Removes bills that are before the Min_Date and after Max_Date
    Categorizes the remaining bills as either pre or post from program date
    '''

    cost_pt_info_df = pd.merge(pat_info[['RecipientID','Program_Date',
                                         'Min_Date','Max_Date','Duration']],pt_w_costs,how='inner')

    cost_pt_info_df = cost_pt_info_df.loc[cost_pt_info_df['ServiceFromDt'].between(cost_pt_info_df['Min_Date'],
                                                                                   cost_pt_info_df['Max_Date'])]

    cost_pt_info_df['Pre_Post'] = cost_pt_info_df['Program_Date']>cost_pt_info_df['ServiceFromDt']
    cost_pt_info_df['Pre_Post'] = cost_pt_info_df['Pre_Post'].replace({True:'Pre',False:'Post'})
    #finds patients_w_no_bills and adds them with a Pre and post value to be pivoted
    pt_no_bills = np.setdiff1d(unique_pt_list,cost_pt_info_df['RecipientID'])
    pt_wo_costs = pat_info.loc[(pat_info['RecipientID'].isin(pt_no_bills)),
                                ['RecipientID','Program_Date','Min_Date','Max_Date','Duration']]
    # adds a Pre and Post for patients w/o bills
    pt_wo_costs['Pre_Post'] = 'Pre'
    pt_wo_costs['CHECK_Category'] = 'Other'
    cost_pt_info_df = pd.concat([cost_pt_info_df,pt_wo_costs])
    pt_wo_costs['Pre_Post'] = 'Post'
    cost_pt_info_df = pd.concat([cost_pt_info_df,pt_wo_costs])
    cost_pt_info_df.fillna(0,inplace=True)
    return cost_pt_info_df


def pp_patient_pivot(pt_cost_df, value_col):
    '''pt_cost_df: pd.DataFrame that has all of '''
    pre_post_pivot = pd.pivot_table(pt_cost_df,index=['RecipientID','Duration','Program_Date'],
                                columns=['Pre_Post','CHECK_Category'],values=value_col,
                                aggfunc=np.sum,fill_value=0)

    pre_post_pivot.columns = [col[1]+"_"+col[0] for col in pre_post_pivot.columns.values]
    pre_post_pivot.reset_index(inplace=True)

    pre_cols = ['ED_Pre', 'Inpatient_Pre', 'Other_Pre', 'Outpatient_Pre', 'Pharmacy_Pre']
    post_cols = ['ED_Post', 'Inpatient_Post', 'Other_Post', 'Outpatient_Post', 'Pharmacy_Post']

    pre_post_pivot['Total_Pre'] = pre_post_pivot[pre_cols].sum(axis=1)
    pre_post_pivot['Total_Post'] = pre_post_pivot[post_cols].sum(axis=1)
    pre_post_pivot['Aggregation_Type'] = value_col

    individual_columns = ['RecipientID','Duration','Aggregation_Type']
    cost_columns = ['Inpatient_Pre','Inpatient_Post','Outpatient_Pre',
                    'Outpatient_Post','ED_Pre','ED_Post','Other_Pre','Other_Post',
                    'Pharmacy_Pre','Pharmacy_Post','Total_Pre','Total_Post']
    # round to 2 decimal places
    pre_post_pivot[cost_columns] = pre_post_pivot[cost_columns].round(2)
    return pre_post_pivot[individual_columns+cost_columns]


def pp_tbl_grouper(individual_data,agg_func,group_by_cols,agg_columns):
    agg_values = {i:agg_func for i in agg_columns}
    agg_values['RecipientID'] = np.count_nonzero
    agg_col = ['Population_Type']
    agg_col += group_by_cols
    group_index = []
    # this makes sure there is a complete index even when the count of a group is equal to 0
    for i in agg_col:
        group_index.append(individual_data[i].unique())
    group_index = itertools.product(*group_index)
    total_group_combinations = pd.DataFrame(index=group_index)
    group_df = individual_data.groupby(agg_col).agg(agg_values)
    group_df['Group_Subset']= group_df.index.get_values()
    group_df.rename_axis({'RecipientID':'N'},axis=1,inplace=True)
    group_df = pd.merge(total_group_combinations,group_df,left_index=True,
                        right_on='Group_Subset',how='left')
    group_df['Group_Type'] = "::".join(agg_col)
    group_df['Aggregation_Function'] = agg_func.__name__
    group_df['Value'] = individual_data['Aggregation_Type'].values[0]
    group_df['Group_Subset'] = group_df['Group_Subset'].apply(lambda x: "::".join(x))
    group_df = group_df.fillna(0)
    group_df = group_df.reset_index(drop=True)
    group_df = group_df[['Group_Type','Group_Subset','Aggregation_Function',
                        'Value','N']+agg_columns]
    return group_df

# runs pre_post
_pre_post_month_window = 6
_preg_age_cutoff = 15
_release_num = 3020
to_sql = True

connector = dbconnect.DatabaseConnect('CHECK_CPAR2')
query = conconnect.ConsensusConnect()
pat_query = """SELECT * FROM rid_pre_post_pat_windows where negative_duration >= {}
                and positive_duration >= {} AND ReleaseNum = {}""".format(_pre_post_month_window,
                                                                         _pre_post_month_window,
                                                                         _release_num)
pt_df = connector.query(pat_query,parse_dates=['Program_Date'])
demo_df = query.cpar_patient_info()
demo_df.rename(columns={'Program_Age_Category':'Age_Cat','Program_Risk':'Risk',
                        'Program_Age':'Age','Preg_Flag':'Pregnancy'},inplace=True)

pat_info = pd.merge(pt_df,demo_df,on=['RecipientID','Program_Date','ReleaseNum'],how='inner')

pat_info['Max_Date'] = pat_info['Program_Date'].apply(lambda x: x + pd.DateOffset(months=_pre_post_month_window))
pat_info['Min_Date'] = pat_info['Program_Date'].apply(lambda x: x - pd.DateOffset(months=_pre_post_month_window))
pat_info['ReleaseNum'] = _release_num
pat_info['Duration'] = _pre_post_month_window

#adds all patients that meet criteria into patient_info
cols = ['RecipientID','Duration','Program_Date','Asthma','Diabetes','SCD',
        'Prematurity','Brain_Injury','Epilepsy','Diagnosis_Category','Risk','Age_Cat','Age','Gender',
        'E2','E4','HC','HE2','HE4','Pregnancy','ReleaseNum']

if to_sql == True:
    connector.insert(pat_info[cols],'rid_pre_post_pat_info')

# for faster claims query selects RecipientIDs
unique_recipientID = pat_info['RecipientID'].unique()

jumper = 3000
agg_columns = ['AdjustedPriceAmt','Visit','VisitInpatientDays']
#iterates through all patients and adds them incrementally to mysql
for x in range(0,len(unique_recipientID),jumper):
    temp_unique_rins = unique_recipientID[x:x+jumper]
    pt_w_costs_df = pp_total_cost_query(temp_unique_rins,connector)
    pt_w_costs_df['CHECK_Category'] = pt_w_costs_df['CHECK_Category'].replace({'OTHER':'Other',
                                                                               'INPATIENT':'Inpatient',
                                                                               'OUTPATIENT':'Outpatient'})
    pt_w_costs_filtered_df = pp_bill_filter(pat_info,pt_w_costs_df,temp_unique_rins)

    for agg_type in agg_columns:
        pre_post_pivot = pp_patient_pivot(pt_w_costs_filtered_df,agg_type)
        pre_post_pivot['ReleaseNum'] = _release_num
        if to_sql == True:
            connector.insert(pre_post_pivot,'rid_pre_post_individual')


individual_data = connector.query("""SELECT * FROM CHECK_CPAR2.rid_pre_post_individual where
                                        ReleaseNum = {} and Duration = {}""".format(_release_num, _pre_post_month_window))
individual_data = pd.merge(individual_data,pat_info,on=['RecipientID','Duration','ReleaseNum'])

cost_columns = ['Inpatient_Pre','Inpatient_Post','Outpatient_Pre',
                'Outpatient_Post','ED_Pre','ED_Post','Other_Pre','Other_Post',
                'Pharmacy_Pre','Pharmacy_Post','Total_Pre','Total_Post']

cols_to_agg = [['Holistic'],['Diagnosis_Category'],['Age_Cat'],['Diagnosis_Category','Age_Cat'],
               ['Risk'],['Risk','Diagnosis_Category'],['Gender','Diagnosis_Category'],['Gender','Pregnancy']]

individual_data['Holistic'] = "1"
individual_data['Pregnancy'] = individual_data['Pregnancy'].astype(str)
df_list = []

agg_columns = ['AdjustedPriceAmt','Visit','VisitInpatientDays']
for agg_type in agg_columns:
    temp = individual_data.loc[individual_data['Aggregation_Type']==agg_type]
    for agg_cols in cols_to_agg:
        group_df = pp_tbl_grouper(temp,np.mean,agg_cols,cost_columns)
        df_list.append(group_df)

group_df = pd.concat(df_list)
group_df[cost_columns] = group_df[cost_columns].round(2)
group_df['ReleaseNum'] = _release_num
group_df['Duration'] = _pre_post_month_window

if to_sql == True:
    connector.insert(group_df,'rid_pre_post_groupings')
