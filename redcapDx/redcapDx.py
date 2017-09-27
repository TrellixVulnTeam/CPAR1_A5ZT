import numpy as np
def redcapDx(enrollmentDF,keepcolumns=None):
    '''Used to make a cleaner diagnosis category, uses labeled data V3 enrollment data from redcap'''
    enrollmentDF = enrollmentDF[['RIN','Asthma','Diabetes','SCD','Prematurity',
                                   'Newborn Brain Injury','Epilepsy','Risk Tier (Utilization Tier)']]
    #Name of columns we will be converting
    col_list = ['Asthma','Diabetes','SCD', 'Prematurity', 'Newborn Brain Injury', 'Epilepsy']
    #Conversion dic will change strings to numbers
    convert_dict = {'No':0,'Yes':1}

    for i in col_list:
        #fills all blanks fields into 'No'
        enrollmentDF[i].fillna('No',inplace=True)
        #conversion occurs here
        enrollmentDF.loc[:,i] = enrollmentDF[i].map(convert_dict)

    #sum against our columns, used to find out who has multiple diagnoses
    enrollmentDF.loc[:,'SumDiagnosis'] = enrollmentDF[col_list].sum(axis=1)
    #new column that will be used to hold diagnosis string
    enrollmentDF.loc[:,'Diagnosis'] = np.nan
    #changed as of 9/17/17 for new diagnosis codes
    #if sum diagnosis > 1 that means pt has more than one diagnosis
    enrollmentDF.loc[enrollmentDF.SumDiagnosis == 0,'Diagnosis'] = 'Other Diagnosis'
    enrollmentDF.loc[((enrollmentDF.Asthma == 1)),'Diagnosis'] = 'Asthma'
    enrollmentDF.loc[((enrollmentDF.Diabetes == 1)),'Diagnosis'] = 'Diabetes'
    enrollmentDF.loc[enrollmentDF.Epilepsy == 1,'Diagnosis'] = 'Neurological'
    enrollmentDF.loc[enrollmentDF['Newborn Brain Injury'] == 1,'Diagnosis'] = 'Neurological'
    enrollmentDF.loc[((enrollmentDF.Prematurity == 1)),'Diagnosis'] = 'Prematurity'
    enrollmentDF.loc[((enrollmentDF.SCD == 1)),'Diagnosis'] = 'SCD'

    if keepcolumns == True:
        enrollmentDF = enrollmentDF[['RIN','Risk Tier (Utilization Tier)','Diagnosis']+col_list]
    else:
        enrollmentDF = enrollmentDF[['RIN','Risk Tier (Utilization Tier)','Diagnosis']]
    enrollmentDF.rename(columns={'Risk Tier (Utilization Tier)':'Risk Tier'},inplace=True)
    return enrollmentDF
