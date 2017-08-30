import pandas as pd

def contactCategorizer(s,precision):
    '''categorizes by successful and unsuccessful and by type of contact home or phone.'''
    contactType = [['Unsuccessful','Successful'],['Home Visit','Phone Contact']]
    if s == 'Wrong Number or Number Disconnected/Out of Service':
        if precision == True:
            return contactType[0][0] + ' ' + contactType[1][1]
        return contactType[0][0]
    elif s == 'Unable to reach':
        if precision == True:
            return contactType[0][0] + ' ' + contactType[1][1]
        return contactType[0][0]
    elif s == 'Successful':
        if precision == True:
            return contactType[0][1] + ' ' + contactType[1][1]
        return contactType[0][1]
    elif s == 'Face to face visit successful':
        if precision == True:
            return contactType[0][1] + ' ' + contactType[1][0]
        return contactType[0][1]
    elif s == 'Left Message':
        if precision == True:
            return contactType[0][0] + ' ' + contactType[1][1]
        return contactType[0][0]
    elif s == 'Face to face visit not successful':
        if precision == True:
            return contactType[0][0] + ' ' + contactType[1][0]
        return contactType[0][0]

def pivotRecentContact(df):
    '''takes in the contact dataframe then pivots to show most recent for each of the contact categorizations'''
    df.sort_values(['MedicaidNum','OutcomeDateTime'],ascending=[True,False],inplace=True)
    df_pivot = df.pivot_table(index=['MedicaidNum','PatientID'],columns='ContactTypeSuccess',
                              values='OutcomeDateTime',aggfunc='first')
    df_pivot['Successful Home Visit'] = pd.to_datetime(df_pivot['Successful Home Visit'].dt.date)
    df_pivot['Successful Phone Contact'] = pd.to_datetime(df_pivot['Successful Phone Contact'].dt.date)
    df_pivot['Unsuccessful Home Visit'] = pd.to_datetime(df_pivot['Unsuccessful Home Visit'].dt.date)
    df_pivot['Unsuccessful Phone Contact'] = pd.to_datetime(df_pivot['Unsuccessful Phone Contact'].dt.date)
    return df_pivot

def contactTypeCounter(df):
    '''takes in contact dataframe and groups by patientID to count of contacts'''
    contact_counts = df.groupby(['PatientID','MedicaidNum','ContactTypeSuccess'],as_index=False).count()
    contact_counts = contact_counts.pivot_table(index=['MedicaidNum','PatientID'],columns='ContactTypeSuccess',values='ContactTS')
    contact_counts.fillna(0,inplace=True)
    col_names = {i:"Count of " + i for i in contact_counts.columns}
    contact_counts.rename(columns=col_names,inplace=True)
    contact_counts.reset_index(inplace=True)
    return contact_counts
