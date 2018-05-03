

def patGroup(df):
    df.loc[df['E2']==1,'Population_Type'] = 'Engaged'
    df.loc[df['E4']==1,'Population_Type'] = 'Enrolled'
    df.loc[df['HC']==1,'Population_Type'] = 'Harmony_Control'
    df.loc[df['HE2']==1,'Population_Type'] = 'Harmony_Engaged'
    df.loc[df['HE4']==1,'Population_Type'] = 'Harmony_Enrolled'
    return df
