import itertools
import numpy as np
import pandas as pd
from CHECK.dbconnect import dbconnect
from CHECK.conconnect import conconnect

class pre_post_analysis():
    def __init__(self,pp_n_months,release_num,db_name):
        '''pp_n_months: (int) Number of months to select pre and post the
        patients program date
        release_num: (int) Release to select from rid_pre_post_pat_windows
        Currently only works for test CHECK_Categories'''
        self.query = conconnect.ConsensusConnect()
        self.pp_n_months = pp_n_months
        self.release_num = release_num
        self.db_name = db_name
        self.connection = dbconnect.DatabaseConnect(self.db_name)
        self.cost_columns = ['Inpatient_Pre','Inpatient_Post','Outpatient_Pre',
                             'Outpatient_Post','ED_Pre','ED_Post','Other_Pre','Other_Post',
                             'Pharmacy_Pre','Pharmacy_Post','Total_Pre','Total_Post']

    def pt_info(self):
        '''Selects patients that have the necessary months of
        data and joins demographic information'''

        pat_query = """SELECT * FROM rid_pre_post_pat_windows where negative_duration >= {}
                        and positive_duration >= {} AND ReleaseNum = {}""".format(self.pp_n_months,
                                                                                  self.pp_n_months,
                                                                                  self.release_num)

        pt_df = self.query.connect(pat_query, db_name=self.db_name, parse_dates=['Program_Date'])

        demo_df = self.query.cpar_patient_info()
        demo_df.rename(columns={'Program_Age_Category':'Age_Cat','Program_Risk':'Risk',
                                'Program_Age':'Age','Preg_Flag':'Pregnancy'},inplace=True)

        pat_info = pd.merge(pt_df, demo_df, on=['RecipientID','Program_Date','ReleaseNum'], how='inner')

        pat_info['Max_Date'] = pat_info['Program_Date'].apply(lambda x: x + pd.DateOffset(months=self.pp_n_months))
        pat_info['Min_Date'] = pat_info['Program_Date'].apply(lambda x: x - pd.DateOffset(months=self.pp_n_months))
        pat_info['ReleaseNum'] = self.release_num
        pat_info['Duration'] = self.pp_n_months

        return pat_info


    def total_cost_query(self, unique_recipientID):
        '''unique_recipientID: pd.Series of RecipientIDs
        Returns all bills for patients that had them, this will remove patients
        that don't have bills '''

        unique_recipientID = ",".join(unique_recipientID)

        claims_query = """SELECT
            RecipientID,
            DCN,
            ServiceFromDt,
            ServiceThruDt,
            CHECK_Category,
            AdjustedPriceAmt,
            Visit,
            Service_Count,
            Procedure_Count,
            Encounter,
            Visit_Inpatient_Days from rid_costs where RecipientID in ({})
            and Window between {} and {}""".format(unique_recipientID,
                                                   -(self.pp_n_months+1),
                                                   self.pp_n_months+1)

        tot_cost_df = self.query.connect(claims_query, db_name=self.db_name,
                                         parse_dates=['ServiceFromDt','ServiceThruDt'])

        tot_cost_df['CHECK_Category'] = tot_cost_df['CHECK_Category'].replace({'OTHER':'Other',
                                                                               'INPATIENT':'Inpatient',
                                                                               'OUTPATIENT':'Outpatient'})
        return tot_cost_df

    def bill_filter(self, pat_info, pt_costs):
        '''pat_info: pd.DataFrame that contains
        pt_w_costs_df: pd.DataFrame with columns
        pt_wo_costs: pd.Series of patients that had no costs
        Returns pd.Dataframe that removed bills  before the Min_Date and after Max_Date
        and categorizes the remaining bills as either pre or post from Program Date
        '''
        cost_pt_info_df = pd.merge(pat_info[['RecipientID','Program_Date',
                                             'Min_Date','Max_Date','Duration']],
                                   pt_costs, how='left', on='RecipientID')
        cost_pt_info_df['Program_Date'] = pd.to_datetime(cost_pt_info_df['Program_Date'])
        cost_pt_info_df = cost_pt_info_df.loc[cost_pt_info_df['ServiceFromDt'].between(cost_pt_info_df['Min_Date'],
                                                                                       cost_pt_info_df['Max_Date'])]
        # Finds pre_post and dates that are within the window
        cost_pt_info_df['Pre_Post'] = cost_pt_info_df['Program_Date'] > cost_pt_info_df['ServiceFromDt']
        cost_pt_info_df['Pre_Post'] = cost_pt_info_df['Pre_Post'].replace({True:'Pre',False:'Post'})
        cost_pt_info_df['Between_Date'] = cost_pt_info_df['ServiceFromDt'].between(cost_pt_info_df['Min_Date'],
                                                                                   cost_pt_info_df['Max_Date'])
        cost_pt_info_df = cost_pt_info_df.loc[cost_pt_info_df['Between_Date'] == True].copy()

        cost_pt_info_df['RecipientID'] = cost_pt_info_df['RecipientID'].astype('category')
        cost_pt_info_df['CHECK_Category'] = pd.Categorical(cost_pt_info_df['CHECK_Category'],
                                                           categories=["ED", "Outpatient",
                                                                       "Inpatient", "Other",
                                                                       "Pharmacy", 'Total'])
        cost_pt_info_df['Pre_Post'] = pd.Categorical(cost_pt_info_df['Pre_Post'],
                                                     categories=["Pre","Post"], ordered=True)

        return cost_pt_info_df

    def patient_pivot(self, pt_cost_df, pat_info, value_col):
        '''pt_cost_df: pd.DataFrame from self.bill_filter
           value_col: Column to contain values i.e. AdjustedPriceAmt, Visit, VisitInpatientDays
           returns pivoted data for RecipientID as index, pre and post CHECK_categories
           as columns with the value col as the summed values'''

        pt_cost_df = pt_cost_df.groupby(['CHECK_Category', 'Pre_Post', 'RecipientID']).sum().fillna(0)

        pt_cost_df = pt_cost_df[['AdjustedPriceAmt','Visit','Service_Count',
                                 'Procedure_Count','Encounter','Visit_Inpatient_Days']]
        pt_cost_df.reset_index(inplace=True)

        pre_post_pivot = pd.pivot_table(pt_cost_df, index=['RecipientID'],
                                        columns=['Pre_Post','CHECK_Category'],
                                        values=value_col, aggfunc='sum')

        pre_post_pivot[('Pre','Total')] = pre_post_pivot['Pre'].sum(axis=1)
        pre_post_pivot[('Post','Total')] = pre_post_pivot['Post'].sum(axis=1)

        pre_post_pivot.columns = [i[1]+"_"+i[0] for i in pre_post_pivot.columns]

        pre_post_pivot = pd.merge(pre_post_pivot, pat_info,
                                  how='right', left_index=True,
                                  right_on='RecipientID').fillna(0)

        pre_post_pivot['Aggregation_Type'] = value_col

        individual_columns = ['RecipientID','Duration','Aggregation_Type']
        # round to 2 decimal places
        pre_post_pivot[self.cost_columns] = pre_post_pivot[self.cost_columns].round(2)
        pre_post_pivot = pre_post_pivot[individual_columns + self.cost_columns]
        pre_post_pivot['ReleaseNum'] = self.release_num
        return pre_post_pivot


    def tbl_grouper(self, individual_data, agg_func, group_by_cols, agg_columns):
        '''individual_data: pd.Dataframe from self.patient_pivot
           agg_func: function to implement
        '''
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
        group_df['Group_Subset'] = group_df.index.get_values()
        group_df.rename(columns={'RecipientID':'N'},inplace=True)
        group_df = pd.merge(total_group_combinations,group_df,left_index=True,
                            right_on='Group_Subset',how='left')
        group_df['Group_Type'] = "::".join(agg_col)
        group_df['Aggregation_Function'] = agg_func.__name__
        group_df['Value'] = individual_data['Aggregation_Type'].values[0]
        group_df['Group_Subset'] = group_df['Group_Subset'].apply(lambda x: "::".join(x))
        group_df = group_df.fillna(0)
        group_df = group_df.reset_index(drop=True)
        group_df = group_df[['Group_Type','Group_Subset','Aggregation_Function',
                            'Value','N'] + agg_columns]
        group_df['Duration'] = self.pp_n_months
        group_df['ReleaseNum'] = self.release_num
        return group_df

    def full_run(self, to_sql):
        '''Runs the pre-post analysis
           to_sql == True will post data to MySQL DB
           Runs a specific groups and iterates through entire patient population that
           so it fits in RAM'''

        pat_info = self.pt_info()
        unique_recipientID = pat_info['RecipientID'].unique()

        pat_info_cols = ['RecipientID','Duration','Program_Date','Asthma','Diabetes','SCD',
                         'Prematurity','Brain_Injury','Epilepsy','Diagnosis_Category','Risk',
                         'Age_Cat','Age','Gender','E2','E4','HC','HE2','HE4','Pregnancy','ReleaseNum']


        agg_columns = ['AdjustedPriceAmt','Visit','Visit_Inpatient_Days']
        pat_piv_df = []
        #iterates through patients in chunks and aggregation types
        pt_costs_df = self.total_cost_query(pat_info['RecipientID'])
        filtered_costs = self.bill_filter(pat_info, pt_costs_df)

        for agg_type in agg_columns:
            pre_post_pivot = self.patient_pivot(filtered_costs, pat_info, agg_type)
            pat_piv_df.append(pre_post_pivot)

        pat_piv_df = pd.concat(pat_piv_df)

        pat_piv_info_df = pd.merge(pat_piv_df, pat_info, on=['RecipientID','Duration','ReleaseNum'])

        cols_to_agg = [['Holistic'],['Diagnosis_Category'],['Age_Cat'],['Diagnosis_Category','Age_Cat'],
                       ['Risk'],['Risk','Diagnosis_Category'],['Gender','Diagnosis_Category'],['Gender','Pregnancy']]

        pat_piv_info_df['Holistic'] = "1"
        pat_piv_info_df['Pregnancy'] = pat_piv_info_df['Pregnancy'].astype(str)
        agg_function = np.mean
        df_list = []
        for agg_type in agg_columns:
            temp = pat_piv_info_df.loc[pat_piv_info_df['Aggregation_Type']==agg_type]
            for agg_cols in cols_to_agg:
                group_df = self.tbl_grouper(temp, agg_function, agg_cols, self.cost_columns)
                df_list.append(group_df)

        group_df = pd.concat(df_list)
        group_df[self.cost_columns] = group_df[self.cost_columns].round(2)


        if to_sql == True:
            self.connection.insert(pat_info[pat_info_cols],'rid_pre_post_pat_info')
            self.connection.insert(pat_piv_df,'rid_pre_post_individual')
            self.connection.insert(group_df,'rid_pre_post_groupings')

        return pat_info, pat_piv_df, group_df
