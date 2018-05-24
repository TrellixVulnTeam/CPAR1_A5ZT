
import pandas as pd
import numpy as np
from CHECK.dbconnect import dbconnect


class DiagnosisMaster(object):

    def __init__(self, database='CHECK_CPAR2'):

        self.connector = dbconnect.DatabaseConnect(database)

        self.table_diagnosis_masters = ['pat_info_dx_mental_health',
                                        'pat_info_dx_pregnancy',
                                        'pat_info_dx_primary']

        self.dx_codes_query = """SELECT p.RecipientID, p.Enrollment_Age, p.Gender,
                                 if(d.RecipientID is null,'0',
                                 GROUP_CONCAT(Distinct DiagCd separator ','))
                                 ICD_List
                                 FROM pat_info_demo p left join
                                 tsc_hfs_diagnosis d
                                 on p.RecipientID = d.RecipientID
                                 group by RecipientID"""

        self.primary_diag_scd_claims_query = """SELECT u.RecipientID,
                    '1' as SCD FROM (SELECT x.RecipientID, MAX(CASE
                    WHEN x.INCL_EXCL = 'I' THEN x.total ELSE 0
                    END) AS INCL, MAX(CASE WHEN x.INCL_EXCL = 'E'
                    THEN x.total ELSE 0 END) AS EXCL
                    FROM
                    (SELECT RecipientID, INCL_EXCL, COUNT(*) AS total
                    FROM tsc_hfs_diagnosis d
                    JOIN dx_code_inc_exc_primary_diagnosis ie
                    ON d.DiagCd = ie.DX_CODE
                    WHERE Group_name = 'SCD' GROUP BY
                    RecipientID , INCL_EXCL) AS x
                    GROUP BY x.RecipientID) AS u
                    WHERE (u.INCL / u.EXCL) <> 0.0
                    AND (u.INCL / u.EXCL) IS NOT NULL
                    AND (u.INCL / u.EXCL) >= 3 ;"""

    def diagnosisCategory(self, df):
        '''function to deduce diagnosis category:
            uses hierarchial diagnosis'''
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

    def inc_exc(self, dx_codes, inc_exc_codes):

        for dx in inc_exc_codes['Group_Name'].unique():

            inc_codes = set(inc_exc_codes.loc[
                             (inc_exc_codes['Group_Name'] == dx)
                             & (inc_exc_codes['Incl_Excl'] == 'I'),
                             'Dx_Code'])

            exc_codes = set(inc_exc_codes.loc[
                                  (inc_exc_codes['Group_Name'] == dx)
                                  & (inc_exc_codes['Incl_Excl'] == 'E'),
                                  'Dx_Code'])

            dx_codes[dx] = dx_codes['ICD_Codes_List'].apply(lambda x: 1 if
            (len(set(x) & inc_codes) > 0) & (len(set(x) & exc_codes) == 0) else 0)

        return dx_codes

    def preg_dx(self, dx_codes):

        inc_exc_tables = self.connector.query( "SELECT * FROM dx_code_inc_exc_pregnancy;")
        dx_codes = self.inc_exc(dx_codes, inc_exc_tables)
        dx_codes.loc[(dx_codes[['Antenatal_care','Delivery','Abortive']].sum(axis=1) > 0) &
                     (dx_codes['Enrollment_Age']>10) & (dx_codes['Gender']=='Female'), 'Preg_Flag'] = 1
        dx_codes.loc[dx_codes[['Antenatal_care','Delivery','Abortive']].sum(axis=1) == 0, 'Preg_Flag'] = 0

        return dx_codes

    def primary_dx(self, dx_codes):

        inc_exc_tables = self.connector.query( "SELECT * FROM dx_code_inc_exc_primary_diagnosis;")
        dx_codes = self.inc_exc(dx_codes, inc_exc_tables)

        pat_scd_clams_info = self.connector.query( self.primary_diag_scd_claims_query)
        dx_codes.loc[:, 'SCD_Claims'] = dx_codes.loc[:, 'SCD']
        dx_codes.loc[dx_codes['RecipientID'].isin(pat_scd_clams_info[
                                            'RecipientID']), 'SCD'] = 1
        dx_codes.loc[dx_codes['Enrollment_Age'] > 3, 'Prematurity'] = 0
        dx_codes['Diagnosis_Category'] = dx_codes.apply(self.diagnosisCategory, axis=1)

        return dx_codes

    def mh_dx(self, dx_codes):
        inc_exc_tables = self.connector.query( "SELECT * FROM dx_code_inc_exc_mental_health;")
        dx_codes = self.inc_exc(dx_codes, inc_exc_tables)
        return dx_codes

    def load_diag_data(self):
        dx_codes = self.connector.query(self.dx_codes_query)
        dx_codes['ICD_Codes_List'] = dx_codes['ICD_List'].str.split(',')

        for table in self.table_diagnosis_masters:
            if table == 'pat_info_dx_primary':
                dx_codes = self.primary_dx(dx_codes)
            if table == 'pat_info_dx_pregnancy':
                dx_codes = self.preg_dx(dx_codes)
            if table == 'pat_info_dx_mental_health':
                dx_codes = self.mh_dx(dx_codes)

            self.connector.replace(dx_codes.drop(['ICD_Codes_List',
                                                   'Enrollment_Age','Gender'],
                                                 axis=1), table)

            dx_codes = dx_codes.drop(dx_codes.iloc[:, 5:], axis=1)

        return 'Diagnosis Categorization completed'
