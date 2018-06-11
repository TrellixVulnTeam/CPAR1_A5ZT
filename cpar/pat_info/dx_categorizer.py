
import pandas as pd
import numpy as np
from CHECK.dbconnect import dbconnect


class DiagnosisMaster(object):

    def __init__(self, database='CHECK_CPAR2'):

        dx_ratio = {'SCD':.75}

        self.connector = dbconnect.DatabaseConnect(database)

        self.diagnosis_tables = ['pat_info_dx_mental_health',
                                        'pat_info_dx_pregnancy',
                                        'pat_info_dx_primary']

        self.pat_info_query = """SELECT p.RecipientID, p.Enrollment_Age, p.Gender,
                                 if(d.RecipientID is null,'0',
                                 GROUP_CONCAT(Distinct DiagCd separator ',')) ICD_List
                                 FROM pat_info_demo p left join
                                 tsc_hfs_diagnosis d
                                 on p.RecipientID = d.RecipientID
                                 group by RecipientID"""

        self.dx_code_query = '''SELECT RecipientID, DiagCd, count(*) ICD_Count
        from tsc_hfs_diagnosis group by RecipientID, DiagCd'''

    def diagnosis_category(self, df):
        '''Categorizes primary diagnosis into a single diagnosis column'''
        if df['ICD_List'] == '0':
            return "NA"
        if df['SCD'] == 1:
            return "SCD"
        if df['Prematurity'] > 0:
            return "Prematurity"
        if df['Epilepsy'] > 0:
            return "Neurological"
        if df['Brain_Injury'] > 0:
            return "Neurological"
        if df['Diabetes'] > 0:
            return "Diabetes"
        if df['Asthma'] > 0:
            return "Asthma"
        else:
            return "Other"

    def inclusion_exclusion_diagnoser(self, pt_dx_codes, dx_inc_exc_table, inc_exc_ratio=1, min_inc_count=1):
        '''
        pt_dx_codes: pd.DataFrame contains counts of all ICD diagnosis that the patient has recorded
        dx_inc_exc_table: pd.DataFrame that contains the inclusion and exclusion codes for a single diagnosis
        inc_exc_ratio: inclusion to exclusion ratio necessary to be diagnosed

        returns a list of patients that met the inclusion exclusion criteria for a diagnosis
        '''

        pt_dx_codes_merge = pd.merge(pt_dx_codes, dx_inc_exc_table, how='inner',
                                     right_on='Dx_Code', left_on='DiagCd')

        inc_exc_rids = pd.pivot_table(pt_dx_codes_merge, index='RecipientID',
                                      columns='Incl_Excl', values='ICD_Count',
                                      aggfunc='sum', fill_value=0)
        if 'E' not in inc_exc_rids.columns:
            inc_exc_rids['E'] = 0
        inc_exc_rids['Inc_Exc_Ratio'] = inc_exc_rids['I'] / (inc_exc_rids['E'] + inc_exc_rids['I'])
        rid_list = inc_exc_rids.loc[(inc_exc_rids['Inc_Exc_Ratio']>=inc_exc_ratio)&
                                    (inc_exc_rids['I']>=min_inc_count)].index

        return rid_list

    def dx_table_iterator(self, inc_exc_table, pat_info):
        '''for a diagnosis family (mh, pregnancy, primary) iterates through all diagnosis
        categories and adds column for each dx subgroup 1 being inclusion 0 being no diagnosis'''

        pat_info_cp = pat_info.copy()
        dx_list = inc_exc_table['Group_Name'].unique()

        for dx in dx_list:
            dx_inc_exc_table = inc_exc_table.loc[inc_exc_table['Group_Name']==dx]
            if dx in dx_ratio:
                ratio = dx_ratio[dx]
            else:
                ratio = 1

            rid_list = self.inclusion_exclusion_diagnoser(dx_codes, dx_inc_exc_table, ratio)
            pat_info_cp.loc[pat_info_cp['RecipientID'].isin(rid_list), dx] = 1
            pat_info_cp[dx].fillna(0, inplace=True)

        pat_info_cp[dx_list] = pat_info_cp[dx_list].astype(int)
        return pat_info_cp

    def primary_dx(self, dx_codes, pat_info):
        '''Prematurity must be less age 3 at Enrollment to be considered Premature'''
        inc_exc_table = self.connector.query( "SELECT * FROM dx_code_inc_exc_primary_diagnosis;")
        pt_dx_table = self.dx_table_iterator(inc_exc_table, pat_info)
        pt_dx_table.loc[pt_dx_table['Enrollment_Age'] > 3, 'Prematurity'] = 0
        pt_dx_table['Diagnosis_Category'] = pt_dx_table.apply(self.diagnosis_category, axis=1)
        return pt_dx_table

    def mh_dx(self, dx_codes, pat_info):
        inc_exc_table = self.connector.query( "SELECT * FROM dx_code_inc_exc_mental_health;")
        pt_dx_table = self.dx_table_iterator(inc_exc_table, pat_info)
        return pt_dx_table

    def preg_dx(self, dx_codes, pat_info):
        '''Determines if pregnancy icd code was ever given to patient
        Should only occur for Females over age 10 at time of enrollment'''

        inc_exc_table = self.connector.query( "SELECT * FROM dx_code_inc_exc_pregnancy;")
        pt_dx_table = self.dx_table_iterator(inc_exc_table, pat_info)

        pt_dx_table.loc[(pt_dx_table[['Antenatal_care','Delivery','Abortive']].sum(axis=1) > 0) &
                     (pt_dx_table['Enrollment_Age']>10) & (pt_dx_table['Gender']=='Female'), 'Preg_Flag'] = 1
        pt_dx_table.loc[pt_dx_table[['Antenatal_care','Delivery','Abortive']].sum(axis=1) == 0, 'Preg_Flag'] = 0

        return pt_dx_table

    def load_diag_data(self, to_sql=True):
        '''Calculates for all of the diagnosis tables and if to_sql is True loads them into the
        database'''
        dx_dfs = {}
        dx_codes = self.connector.query(self.dx_code_query)
        pat_info = self.connector.query(self.pat_info_query)

        for table in self.diagnosis_tables:
            if table == 'pat_info_dx_primary':
                pat_dx_table = self.primary_dx(dx_codes, pat_info)
            elif table == 'pat_info_dx_pregnancy':
                pat_dx_table = self.preg_dx(dx_codes, pat_info)
            elif table == 'pat_info_dx_mental_health':
                pat_dx_table = self.mh_dx(dx_codes, pat_info)

            dx_dfs[table] = pat_dx_table

        if to_sql == True:
            for table in dx_dfs.keys():
                self.connector.replace(dx_dfs[table].drop(['Enrollment_Age','Gender'], axis=1), table)
                print("{} diagnosis table replaced".format(table))
            return 'Diagnosis Categorization completed'
        else:
            return dx_dfs
