def contactCategorizer(s,precision):
    '''categorizes by successful and unsuccessful and by type of contact home or phone. '''
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
    return df_pivot
