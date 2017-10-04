import pandas as pd
def cpsDateCleaner(s):
    loc = s.find('/')
    s = s[:4] +'-'+ s[loc-2:loc]+'-'+s[loc+1:loc+3]
    s = pd.to_datetime(s,format='%Y-%m-%d')
    return s

def attendanceCalc(df,chronic_col=False):
    df['Day_Count'] = df['Present']+df['Absent']
    df['Att_Percent'] = df['Present']/df['Day_Count']
    df['Chronic_Absent'] = df['Att_Percent']<.9

def columnClean(df):
    '''Renames the columns so they all have the same capitalized and _ format between column words'''
    col_rename = {'EngagementDate':'Engagement_Date','FaerDiagnosis':'Faer_Diagnosis','Student_ID':'Student_ID',
    'PatientID':'Patient_ID','RIN':'RIN','scd':'SCD','dob':'DOB'}
    for i in list(df.columns):
        if i not in list(col_rename.keys()):
            col_rename[i] = "_".join(i.split('_')).title()
        else:
            continue
    return col_rename
