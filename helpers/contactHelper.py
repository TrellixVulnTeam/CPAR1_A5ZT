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
    elif s == 'Declined/Opted Out of Program':
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

def reasonClean(s):
    s = s.replace('-',' ')
    s = s.replace('Care Coordination Consult ','Care Coordination Consultation ')
    s = " ".join(s.split())
    s = s.title()
    return s
    
#category = "A.S.C."
asc_category = ['Tier 1 Assessment', 'Tier 2 Assessment', 'Tier 2 Follow Up Assessment', 'Tier 3 Assessment',
                'Tier 3 Follow Up Assessment', 'Clinical Interview', 'Developmental Screening/Services',
                'Discharge Planning', 'Ed Follow Up','Ed/Hospitalization Follow Up', 'Initial Patient Contact',
                'Notice Of Hospital Admission', 'Notification Hospital Admit/Discharge',
                'Patient Visit', 'Review Of Care Plan','Review/Modification Of Care Plan']

#category = "Consultations
consultations = ['Care Coordination Consultation With Pcp','Care Coordination Consultation With Physician',
                          'Care Coordination Consultation With Specialist', 'Care Coordination Consultation With Pcp',
                          'Case Consultation','Clinical Rounds', 'Complaint',
                          'Coordinate With Pcp/Specialist', 'Coordination/Information']

#category = Health Promotion/Education
health_promotion_education = ['Address Medical/Clinical Concern Or Issue','Appointment Coordination',
                              'Appointment Follow Up','Appointment Reminder', 'Appointment Scheduling',
                              'Care Coordination', 'Care Coordination Follow Up','Care Coordination Plan',
                              'Child Skills Group', 'Dental Concern', 'Dental Concern Or Issue','Dvd Follow Up',
                              'Dental Concern','Dental Concern Or Issue','Education Session','In Person Intervention',
                              'Medication /Prescription Concerns','Parenting Skills Group', 'Portal', 'Portal Information',
                              'Skill Building Intervention','Suicide Protocol/Crisis Intervention'
                              ,'The Happiest Baby Intervention']
#category = Maintenance
maintenance = ['Address Enrollment Questions','Birthday','Enrollment','Incentive','Information Received',
               'Medical Records','Medical/Clinical Concern Or Issue','Other See Notes','Response To Utr Letter',
               'Return Call','Returned Mail','Satisfaction Survey','Unable To Reach Letter', 'Unable To Reach Via Phone']
#category = Referrals
referrals = ['Change of PCP or Specialist','Community Resource Coordination/Information',
             'Community Resource Information', 'Community Resources','Dental Appointment Coordination',
             'Insurance Information', 'Insurance Question','Mail From Social Service Agency',
             'Mental Health Concern Or Follow Up','Mental Health Concern Or Issue','Pharmacy Concern',
             'Pharmacy Concern Or Issue','Pharmacy Insurance Concerns', 'Referral', 'Referral Follow Up',
             'Transportation','Transportation Concern', 'Transportation Coordination']


def serviceCategory(reason):
    category = ""

    if reason in asc_category:
        category = "A.S.C."
    elif reason in consultations:
        category = 'Consultations'
    elif reason in health_promotion_education:
        category = 'Health Promotion/Education'
    elif reason in maintenance:
        category = 'Maintenance'
    elif reason in referrals:
        category = 'Referrals'
    else:
        category = 'Nothing'
    return category
