import itertools
import math
import os
import numpy as np
import pandas as pd
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from CHECK.conconnect import conconnect
from CHECK.dbconnect import dbconnect


class rolling_window_analysis():

    def __init__(self,db_name):
        self.query = conconnect.ConsensusConnect()
        self.db_name = db_name

    def claims_query(self, pat_info_df, recipient_list):
        '''Selects all claims from recipient_list, joins the pat_info_df '''
        rid_str = ",".join(recipient_list)

        claims_query = """SELECT * from rid_costs where RecipientID in ({})""".format(rid_str)

        tot_cost_df = self.query.connect(claims_query, db_name=self.db_name,
                                    parse_dates=['ServiceFromDt','ServiceThruDt'])

        tot_cost_df['Category3'] = tot_cost_df['Category1'] + '_' + tot_cost_df['Category2'] + '_' + tot_cost_df['Category3']
        tot_cost_df['Category2'] = tot_cost_df['Category1'] + '_' + tot_cost_df['Category2']

        tot_cost_df = pd.merge(tot_cost_df,pat_info_df[['RecipientID','Program_Date']], on='RecipientID',how='left')
        tot_cost_df['Window']  = ((tot_cost_df['ServiceFromDt']-tot_cost_df['Program_Date'])
                                   / np.timedelta64(1, 'M'))
        tot_cost_df['Window'] = tot_cost_df['Window'].apply(math.ceil)
        tot_cost_df.fillna(0,inplace=True)
        return tot_cost_df

    def to_rolling_pivot(self, tot_cost_df, category_col):
        '''tot_cost_df: pd.DataFrame from claims_query that contains all claims for patient set
        category_col: Column name in tot_cost_df that will get values for in each window
        Ouputed data will have rows for each patient, window and category and the values associated.
        If inserting Category1 and CHECK_Category watch out they have same values cate gory columns'''
        # The method pivots each type of subgroup in category_col for a patient it will not have rows for patients
        # that have no costs in a window year sum data
        n_months = 12
        category_values = tot_cost_df[category_col].unique()
        fst_pivot = pd.pivot_table(tot_cost_df,index=category_col,columns=['RecipientID','Window'],
                                   values=['AdjustedPriceAmt','VisitInpatientDays',
                                           'Encounter','ServiceCount'],aggfunc='sum')
        fst_pivot = fst_pivot.fillna(0)
        fst_pivot.loc['TOTAL'] = fst_pivot.sum()
        fst_pivot = fst_pivot.T.reset_index()
        fst_pivot.rename(columns={"level_0":'Agg_Col'},inplace=True)
        # Pivot sets RecipientID as columns... this will have NaN for windows that do not have costs
        pat_window_pivot = pd.pivot_table(fst_pivot,index=['Window'],columns=['Agg_Col','RecipientID'],
                                          values=category_values, aggfunc='first')

        pat_window_pivot = pat_window_pivot.reorder_levels(['Agg_Col','RecipientID',category_col],axis=1)
        pat_window_pivot = pat_window_pivot.sort_index(axis=1,level=1)
        # The window parameter requires n_months of non-Nan values to keep summing; windows with
        # NaNs will continue to sum until there are n_months NaNs in a row; which would indicate they could be
        # ineligibile for state benfits .
        year_rolling_sum = pat_window_pivot.rolling(window=n_months, min_periods=1,axis=0).sum()
        year_rolling_sum = pd.DataFrame(year_rolling_sum.unstack(),columns=['Agg_Value']).dropna()
        year_rolling_sum.reset_index(inplace=True)
        # Pivots to long format with moving category_col as a column
        year_rolling_sum = pd.pivot_table(year_rolling_sum,index=['RecipientID',category_col,'Window'],
                                          columns='Agg_Col',values='Agg_Value',aggfunc='first')
        year_rolling_sum = year_rolling_sum.reset_index()
        year_rolling_sum['AdjustedPriceAmt'] = year_rolling_sum['AdjustedPriceAmt'].round(2)
        # Removes the first n_months windows that were not actually summed costs for the amount of time
        grouper = year_rolling_sum.groupby(['RecipientID',category_col])
        year_rolling_sum = year_rolling_sum[grouper['Window'].apply(lambda x: x>=min(x)+(n_months-1))]
        year_rolling_sum.rename(columns={category_col:'Category'},inplace=True)
        year_rolling_sum['Category_Type'] = category_col

        return year_rolling_sum

    def load_files(self,directory):
        '''loads all csv into database'''
        directory = os.listdir(directory)
        for i in directory:
            file_name = directory + i
            load_file = """LOAD DATA LOCAL INFILE '{}' INTO TABLE rid_rw_pt_cat
            FIELDS TERMINATED BY ','
            LINES TERMINATED BY '\n'
            IGNORE 1 LINES""".format(file_name)
            connector.inline_import(load_file,file_name)
        print('upload to MySQL')

    def aggregation_df(self, parquet=False):
        '''returns df that contains all of the patient level data
        When parquet is equal to True it copies data over to a parquet file format
        which is faster for querying'''

        df = dd.read_csv('./rolling_window_output/pt_level_*.csv')
        df = df.categorize(columns=['Window', 'Category_Type','Category','Population_Type',
                                    'Diagnosis_Category','Diagnosis_Category','Age_Category','Gender'])

        df['AdjustedPriceAmt'] = df['AdjustedPriceAmt'].astype(float)
        df['Encounter'] = df['Encounter'].astype(int)
        df['ServiceCount'] = df['ServiceCount'].astype(int)
        df['VisitInpatientDays'] = df['VisitInpatientDays'].astype(int)
        if parquet == True:
            dd.to_parquet(df,'rolling_window_parquet')
            df = dd.read_parquet('rolling_window_parquet/')
        return df

    def window_aggregation(self, df, group):
        '''
        df: dd.Dataframe that is made from aggregation_df
        group: list columns to groupby
        roups columns and prepares dataframe for sql'''

        group_tot = group + ['Category_Type','Category','Window']
        group_df = df.groupby(group_tot)
        df_calc = group_df[['AdjustedPriceAmt', 'Encounter',
                            'ServiceCount', 'VisitInpatientDays']].agg({'AdjustedPriceAmt': ['count','mean','std'],
                                                                       'Encounter': 'mean','ServiceCount':'mean',
                                                                       'VisitInpatientDays':'mean'})
        a = "_".join(group)
        with ProgressBar():
            print(a)
            group_output = df_calc.compute()

        group_output.columns = [col[0]+"_"+col[1].title() for col in group_output.columns.values]
        group_output = group_output.reset_index()
        group_output['Group_Type'] = "::".join(group)
        group_output['Group_Subset'] = group_output[group].apply(lambda x: '::'.join(x), axis=1)

        if a == '':
            a = 'Holistic'
            group_output['Group_Subset'] = 'Holistic'
            group_output['Group_Type'] = 'Holistic'

        group_output['Window'] = group_output['Window'].astype(int)
        group_output = group_output.sort_values(['Group_Subset','Category','Window'])
        group_output = group_output.rename(columns={'AdjustedPriceAmt_Count':'N'})
#         group_output = group_output[['Group_Type','Group_Subset','Category_Type','Category','Window','N',
#                                       'AdjusterPriceAmt_Mean','AdjustedPriceAmt_Std','Encounter_Mean',
#                                       'ServiceCount_Mean','VisitInpatientDays_Mean','VisitInpatientDays_Std']]
        return group_output




    def full_run(self,recipient_list=None,to_sql=False):

        demo_df = self.query.cpar_patient_info()
        pat_program_dates = demo_df[['RecipientID', 'Program_Date']].copy()
        demo_df = demo_df[['RecipientID', 'Population_Type', 'Diagnosis_Category',
                           'Program_Risk', 'Program_Age_Category', 'Gender']]
        demo_df = demo_df.set_index('RecipientID')
        demo_df.rename(columns={'Program_Risk':'Risk',
                                'Program_Age_Category':'Age_Category'},inplace=True)

        connector = dbconnect.DatabaseConnect('CHECK_CPAR2')
        if recipient_list is None:
            recipient_list = demo_df.index.unique()

        jumper = 500
        cat_col_list = ['CHECK_Category', 'Category1', 'Category2', 'Category3']

        output_path = "rolling_window_output/"
        for x in range(0,len(recipient_list),jumper):

            file_name = "{}pt_level_{}.csv".format(output_path,x)
            rolling_df = []
            temp_unique_rins = recipient_list[x:x+jumper]
            pt_costs_df = self.claims_query(pat_program_dates,temp_unique_rins)
            for cat_col in cat_col_list:
                rolling_win = self.to_rolling_pivot(pt_costs_df,cat_col)
                rolling_win = rolling_win.set_index('RecipientID')
                rolling_win = pd.merge(rolling_win,demo_df,left_index=True,right_index=True)
                rolling_df.append(rolling_win)

            rolling_df = pd.concat(rolling_df)
            rolling_df = rolling_df[['Population_Type','Diagnosis_Category','Risk','Age_Category','Gender',
                                     'Category_Type','Category','Window','AdjustedPriceAmt',
                                     'Encounter','ServiceCount','VisitInpatientDays']]

            rolling_df.to_csv(file_name,chunksize=100000)

        print('Completed rolling window calculation')

        grouping_list = [[],['Diagnosis_Category'],['Risk'],['Age_Category'],['Diagnosis_Category','Risk'],
                         ['Diagnosis_Category','Age_Category'],['Population_Type','Diagnosis_Category'],
                         ['Population_Type','Diagnosis_Category','Age_Category'],['Population_Type','Gender'],
                         ['Diagnosis_Category','Risk','Age_Category'],['Population_Type','Risk']]

        if 'agg_output' not in os.listdir():
            os.mkdir('agg_output')

        df = self.aggregation_df()
        for group in grouping_list:
            print(group)
            group_output = self.window_aggregation(df, group)
            group_output.to_csv('agg_output/Aggregation_' + "_".join(group) + '.csv')

        if to_sql == True:
            load_files(output_path)

        return 'completed'
