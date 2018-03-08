import datetime
import numpy as np
import pandas as pd
from collections import OrderedDict
from CHECK.conconnect import ConsensusConnect
from CHECK.helpers import PhoneMapHelper


query = ConsensusConnect.ConsensusConnect()
red_cap = query.redcapImport(dropCol=False)
demo = query.totalDemo()
participant_del = query.cps_delta(table='participant')
participant_del['RIN'] = participant_del['PARTICIPANT_SYSTEM_ID'].apply(PhoneMapHelper.medicaidNormalizer)

#select patients that are not already in the participiation delta table
participant_delta_append = red_cap.loc[~red_cap['RIN'].isin(participant_del['RIN'])].copy()
#selects those that have enrollment information
participant_delta_append = participant_delta_append.loc[participant_delta_append['RIN'].isin(demo['RIN'])]


#rename and select start date and begin date
start_end_dates = demo[['RIN','First_Enrollment_Date','DisEnrollment_Date']]
start_end_dates = start_end_dates.rename(columns={'First_Enrollment_Date':'BEGIN_DATE','DisEnrollment_Date':'END_DATE'})
#membership delta columns
member_delta_append = pd.merge(participant_delta_append,start_end_dates,left_on='RIN',right_on='RIN',how='left')
member_delta_append = member_delta_append[['RIN','BEGIN_DATE','END_DATE']]
member_delta_append['DROP_DATE'] = np.nan
member_delta_append['MEMBERSHIP_ENTRY_REASONS'] = np.nan
member_delta_append['MEMBERSHIP_EXIT_REASONS'] = np.nan
member_delta_append['MEMBERSHIP_REFERENCE'] = np.nan
member_delta_append.rename(columns={'RIN':'PARTICIPANT_SYSTEM_ID'},inplace=True)
member_delta_append['PARTICIPANT_SYSTEM_ID'] = member_delta_append['PARTICIPANT_SYSTEM_ID'].str.lstrip('0')
#make sure begin date and end date column as as datetime
member_delta_append['BEGIN_DATE'] = pd.to_datetime(member_delta_append['BEGIN_DATE'])
member_delta_append['END_DATE'] = pd.to_datetime(member_delta_append['END_DATE'])
member_delta_append['DROP_DATE'] = pd.to_datetime(member_delta_append['DROP_DATE'])

id_column = participant_delta_append['RIN'].str.lstrip('0')

participant_columns = OrderedDict({'PARTICIPANT_SYSTEM_ID': id_column,
                                   'PARTICIPANT_CPS_ID':np.nan,
                                   'PARTICIPANT_CHA_ID':np.nan,
                                   'PARTICIPANT_STATE_ID':np.nan,
                                   'PARTICIPANT_LOCAL_ID':id_column,
                                   'FIRST_NAME':participant_delta_append['fn'],
                                   'MIDDLE_NAME':np.nan,
                                   'LAST_NAME':participant_delta_append['ln'],
                                   'BIRTH_DATE':participant_delta_append['dob'],
                                   'BIRTH_MONTH':np.nan,
                                   'BIRTH_DAY':np.nan,
                                   'BIRTH_YEAR':np.nan,
                                   'PARTICIPANT_GENDER':participant_delta_append['gender'],
                                   'PARTICIPANT_RACE':np.nan,
                                   'DISTRICT_CODE':np.nan,
                                   'CURRENT_GRADE_LEVEL':np.nan,
                                   'CURRENT_SCHOOL':np.nan,
                                   'MOTHER_MAIDEN_NAME':np.nan,
                                   'STREET_NUMBER':np.nan,
                                   'STREET_DIRECTION':np.nan,
                                   'STREET_NAME':np.nan,
                                   'STREET_TYPE':np.nan,
                                   'UNIT':np.nan,
                                   'CITY':np.nan,
                                   'STATE_CODE':np.nan,
                                   'POSTAL_CODE':participant_delta_append['zip_code'],
                                   'COUNTY_CODE':np.nan,
                                   'COUNTY':np.nan,
                                   'PHONE_NUMBER':np.nan,
                                   'EMAIL_ADDRESS':np.nan,
                                   'UPDATE_DATE':np.nan,
                                   'age':np.nan})

for i in participant_columns:
   participant_delta_append[i] = participant_columns[i]

participant_delta_append = participant_delta_append[list(participant_columns.keys())]

participant_delta_append['BIRTH_DATE'] = pd.to_datetime(participant_delta_append['BIRTH_DATE'])
participant_delta_append['UPDATE_DATE'] = pd.to_datetime(participant_delta_append['UPDATE_DATE'])
participant_delta_append['age'] = datetime.datetime.today()-participant_delta_append['BIRTH_DATE']
participant_delta_append['age'] = participant_delta_append['age']/np.timedelta64(1, 'Y')
participant_delta_append['age'] = participant_delta_append['age'].astype(int)


toSQL.toSQL(member_delta_append,exist_method='append',table='cps_membership_delta')
toSQL.toSQL(participant_delta_append,exist_method='append',table='cps_participant_delta')
