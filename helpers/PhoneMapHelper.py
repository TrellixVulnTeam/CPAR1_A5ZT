import numpy as np

def zipConvert(zipCode):
    if len(str(zipCode)) > 5:
        zipCode = str(zipCode)[:5]
    return zipCode

def diagnosisConvert(a):
    if len(a)>0:
        return 1
    else:
        return 0

def diagnosisCleaner(s):
    s = ",".join(s.split())
    a = s.strip()
    a = a.replace("_"," ")
    return a

def ageGrouper(age):
    if age <= 1:
        return 'THB'
    elif age <= 5:
        return 'THT'
    elif age >= 13 and age <= 19:
        return 'Teen'
    else:
        return ''

def medicaidNormalizer(s):
    s = int(s)
    s = str(s)
    s = s.strip()
    while len(s)<9:
        s = '0'+s
    return s

def redcapDiagnosis(df):
    '''Converts redcap binary diagnosis columns to a string categorization,'''
    df['FaerDiagnosis'] = ""
    df['Diagnosis'] = np.nan
    #converts string diagnosis into 1 or 0
    df['other_diag'].fillna('',inplace=True)
    df['other_diag'] = df['other_diag'].apply(diagnosisConvert)
    #if pt has epilepsy or is newborn the patient goes under other diagnosis
    df.loc[df[['epilepsy','newborn']].sum(axis=1) > 0,'other_diag'] = 1

    diag_list = ['asthma','diabetes','scd','prematurity','newborn','epilepsy']
    for i in diag_list:
        df[i].fillna(0,inplace=True)
        df[i] = df[i].astype(int)

    df['SumDiagnosis'] = df[['asthma','diabetes','scd','prematurity',
                                                     'other_diag']].sum(axis=1)
    #if sum diagnosis > 1 that means pt has more than one diagnosis
    #loc[selects rows on conditions,column to be set to new value] = value to set
    #these patients only have one diagnosis so given appropriate diagnosis
    df.loc[df.SumDiagnosis == 0,'other_diag'] = 1
    df.loc[(df.epilepsy == 1),'other_diag'] = 1
    df.loc[(df.newborn == 1),'other_diag'] = 1
    df.loc[(df.other_diag == 1),'FaerDiagnosis'] += ' "Other_Diagnosis" '
    df.loc[(df.diabetes == 1),'FaerDiagnosis'] += ' "Diabetes" '
    df.loc[(df.asthma == 1),'FaerDiagnosis'] += ' "Asthma" '
    df.loc[(df.scd == 1),'FaerDiagnosis'] += ' "SCD" '
    df.loc[(df.prematurity == 1),'FaerDiagnosis'] += ' "Prematurity" '

    df.loc[df.epilepsy == 1,'Diagnosis'] = 'Other Diagnosis'
    df.loc[df.newborn == 1,'Diagnosis'] = 'Other Diagnosis'
    df.loc[df.prematurity == 1,'Diagnosis'] = 'Other Diagnosis'
    df.loc[df.SumDiagnosis > 1,'Diagnosis'] = 'Multiple Diagnoses'
    #these patients only have one diagnosis so given appropriate diagnosis
    df.loc[df.SumDiagnosis == 0,'Diagnosis'] = 'Other Diagnosis'
    df.loc[((df.diabetes == 1)&(df.SumDiagnosis == 1)),'Diagnosis'] = 'Diabetes'
    df.loc[((df.asthma == 1)&(df.SumDiagnosis == 1)),'Diagnosis'] = 'Asthma'
    df.loc[((df.scd == 1)&(df.SumDiagnosis == 1)),'Diagnosis'] = 'SCD'
    df.loc[((df.prematurity == 1)&(df.SumDiagnosis == 1)),'Diagnosis'] = 'Prematurity'

    #all others are given multiple diagnosis values and then fills the NA values inplace!
    df['FaerDiagnosis'] = df['FaerDiagnosis'].apply(diagnosisCleaner)

    return df
