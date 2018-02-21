
import pandas as pd
import numpy as np
import sqlalchemy
from dbconnect import dbconnect

connector = dbconnect.DatabaseConnect('CHECK_CPAR2')

table_dx_codes = 'tsc_hfs_diagnosis'
table_inc_exc_codes = ['dx_code_inc_exc_mental_health','dx_code_inc_exc_pregnancy','dx_code_inc_exc_primary_diagnosis']
table_diagnosis_masters = ['pat_info_dx_mental_health','pat_info_dx_pregnancy','pat_info_dx_primary']

dx_codes_query = """
SELECT
    p.RecipientID,
    IF(d.RecipientID IS NULL,
        '0',
        GROUP_CONCAT(DISTINCT DiagCd
            SEPARATOR ',')) ICD_List
FROM
    pat_info_demo p
        LEFT JOIN
    tsc_hfs_diagnosis d ON p.RecipientID = d.RecipientID
GROUP BY RecipientID"""

primary_diag_scd_claims_query = """SELECT
    u.RecipientID, '1' as SCD
FROM
    (SELECT
        x.RecipientID,
            MAX(CASE
                WHEN x.INCL_EXCL = 'I' THEN x.total
                ELSE 0
            END) AS INCL,
            MAX(CASE
                WHEN x.INCL_EXCL = 'E' THEN x.total
                ELSE 0
            END) AS EXCL
    FROM
        (SELECT
        RecipientID, INCL_EXCL, COUNT(*) AS total
    FROM
        tsc_hfs_diagnosis d
    JOIN dx_code_inc_exc_primary_diagnosis ie ON d.DiagCd = ie.DX_CODE
    WHERE
        Group_name = 'SCD'
    GROUP BY RecipientID , INCL_EXCL) AS x
    GROUP BY x.RecipientID) AS u
WHERE
    (u.INCL / u.EXCL) <> 0.0
        AND (u.INCL / u.EXCL) IS NOT NULL
        AND (u.INCL / u.EXCL) >= 3 ;"""


def diagnosisCategory(df):
    '''function to deduce diagnosis category: uses hierarchial diagnosis'''
    if df['ICD_List'] == '0':
         return "NA"
    if df['SCD'] == 1:
        return "SCD"
    if df['Prematurity'] > 0:
        return "Prematurity"
    if df['Epilepsy'] > 0 :
        return "Neurological"
    if df ['Brain_Injury'] > 0:
        return "Neurological"
    if df['Diabetes'] > 0:
        return "Diabetes"
    if df['Asthma'] > 0:
        return "Asthma"
    else:
        return "Other"

dx_codes = connector.query(dx_codes_query)
dx_codes['ICD_Codes_List'] = dx_codes['ICD_List'].str.split(',')
index = 0

for table in table_inc_exc_codes:

    inc_exc_codes = connector.query("SELECT * FROM {};".format(table))

    for dx in inc_exc_codes['Group_Name'].unique():

        inc_codes = set(inc_exc_codes.loc[(inc_exc_codes['Group_Name']==dx)
                              &(inc_exc_codes['Incl_Excl']=='I'),
                             'Dx_Code'])
        exc_codes = set(inc_exc_codes.loc[(inc_exc_codes['Group_Name']==dx)
                              &(inc_exc_codes['Incl_Excl']=='E'),
                             'Dx_Code'])
    #gives 0 if patient has any exclusion code or does not have an inclusion code
        dx_codes[dx] = dx_codes['ICD_Codes_List'].apply(lambda x: 1 if (len(set(x) & inc_codes)>0)&
                                                            (len(set(x) & exc_codes)==0) else 0)

    # calculate SCD from SCD_claims and determine diagnosis_category
    if table == 'dx_code_inc_exc_primary_diagnosis':
        pat_scd_clams_info = connector.query(primary_diag_scd_claims_query)
        dx_codes.loc[:,'SCD_Claims'] = dx_codes.loc[:,'SCD']
        dx_codes.loc[dx_codes['RecipientID'].isin(pat_scd_clams_info['RecipientID']),'SCD'] = 1
        dx_codes['Diagnosis_Category'] = dx_codes.apply(diagnosisCategory,axis=1)
    if table == 'dx_code_inc_exc_pregnancy':
        dx_codes.loc[dx_codes.sum(axis=1)>0,'Preg_Flag'] = 1
        dx_codes['Preg_Flag'].fillna(0,inplace=True)
    # insert into database
    connector.replace(dx_codes.drop('ICD_Codes_List', axis=1), table_diagnosis_masters[index])
    # drop the columns
    dx_codes = dx_codes[['RecipientID','ICD_List','ICD_Codes_List']]
    index += 1
