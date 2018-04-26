import pandas as pd
def cpsDateCleaner(s):
    loc = s.find('/')
    s = s[:4] +'-'+ s[loc-2:loc]+'-'+s[loc+1:loc+3]
    s = pd.to_datetime(s, format='%Y-%m-%d')
    return s

def attendanceCalc(df,chronic_col=False):
    df['Day_Count'] = df['Present']+df['Absent']
    df['Att_Percent'] = df['Present']/df['Day_Count']
    df['Chronic_Absent'] = df['Att_Percent']<.9

def columnClean(df):
    '''Renames the columns so they all have the same capitalized and _ format between column words'''
    col_rename = {'EngagementDate':'Engagement_Date','FaerDiagnosis':'Faer_Diagnosis','STUDENT_ID':'Student_ID',
    'PatientID':'Patient_ID','Medicaid Rin':'RIN','scd':'SCD','dob':'DOB'}
    for i in list(df.columns):
        if i not in list(col_rename.keys()):
            col_rename[i] = "_".join(i.split('_')).title()
        else:
            continue
    return col_rename

def cpsQuarters(df):
    '''quarter start dates amd end dates'''

    df.loc[df['Date'].between('2014-09-02','2014-11-06'),'Quarter'] = 'Q1'
    df.loc[df['Date'].between('2014-11-07','2015-01-29'),'Quarter'] = 'Q2'
    df.loc[df['Date'].between('2015-01-30','2015-04-02'),'Quarter'] = 'Q3'
    df.loc[df['Date'].between('2015-04-03','2015-06-16'),'Quarter'] = 'Q4'

    df.loc[df['Date'].between('2015-09-06','2015-11-12'),'Quarter'] = 'Q1'
    df.loc[df['Date'].between('2015-11-13','2016-02-04'),'Quarter'] = 'Q2'
    df.loc[df['Date'].between('2016-02-05','2016-04-07'),'Quarter'] = 'Q3'
    df.loc[df['Date'].between('2016-04-08','2016-06-21'),'Quarter'] = 'Q4'

    df.loc[df['Date'].between('2016-08-28','2016-11-03'),'Quarter'] = 'Q1'
    df.loc[df['Date'].between('2016-11-04','2017-02-02'),'Quarter'] = 'Q2'
    df.loc[df['Date'].between('2017-02-03','2017-04-06'),'Quarter'] = 'Q3'
    df.loc[df['Date'].between('2017-04-07','2017-06-20'),'Quarter'] = 'Q4'

    df.loc[df['Date'].between('2017-08-27','2017-11-02'),'Quarter'] = 'Q1'
    df.loc[df['Date'].between('2017-11-03','2018-02-01'),'Quarter'] = 'Q2'
    df.loc[df['Date'].between('2018-02-02','2018-04-12'),'Quarter'] = 'Q3'
    df.loc[df['Date'].between('2018-04-13','2018-06-18'),'Quarter'] = 'Q4'

    return df

def grade_group(s):
    if s in ['PK','PE']:
        s = '<K'
    elif s in ['K','1','2','3','4','5']:
        s = 'K-5'
    elif s in ['6','7','8']:
        s = '6-8'
    elif s in ['9','10','11','12']:
        s = '9-12'
    return s
