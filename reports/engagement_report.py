
# Put most updated redcap into Consensus prior to running!
import pandas as pd
from conconnect import ConsensusConnect
from conconnect import toSQL
from helpers import PhoneMapHelper
import pymysql
import datetime
import numpy as np

class UpdateTables():
    def engagement_report(self,cut_end_date):
        """updates the engagementDate table,
        requires cut_end_date which is the last date of the previous month (yyyy-mm-dd)"""

        #Tier 1 date is the date the patient is cosindered engaged, selects the first completed tier 1 assessment date.
        query = ConsensusConnect.ConsensusConnect()
        tier_1_date = query.tier1Date()

        #Selects the engagement to see who already is engaged
        engagement_dates = query.engagementDate()

        #patient must be in red cap before being considered engaged
        redcap = query.redcapImport()

        tier_1_date['Medicaid ID'] = tier_1_date['Medicaid ID'].apply(PhoneMapHelper.medicaidNormalizer)
        engagement_dates['RIN'] = engagement_dates['RIN'].apply(PhoneMapHelper.medicaidNormalizer)

        #beginning of project
        cut_start_date = '2014-01-01'

        tier_1_date['Medicaid ID'] =  tier_1_date['Medicaid ID'].astype('str')
        redcap['RIN']  = redcap['RIN'].astype('str')
        tier_1_date = tier_1_date.loc[tier_1_date['Medicaid ID'].isin(redcap['RIN'])]

        #selects all tier 1 from date of assessment to current date
        cut_end_date = datetime.date.today()

        engagement_date_cut = tier_1_date.loc[(tier_1_date.StartDate >= pd.to_datetime(cut_start_date))&
                                                  (tier_1_date.StartDate <= pd.to_datetime(cut_end_date)),:]

        #selects patients that are in redcap; required to be a valid pt
        engagement_dates = engagement_dates.loc[engagement_dates['RIN'].isin(redcap['RIN'])]
        #and is not already within the engagementDate table
        fresh_engagement_dates = tier_1_date.loc[~(tier_1_date['Medicaid ID'].isin(engagement_dates['RIN']))].copy()
        fresh_engagement_dates['StartDate'] = fresh_engagement_dates['StartDate'].apply(lambda x: x.date())
        fresh_engagement_dates.rename(columns={'StartDate':'EngagementDate','Medicaid ID':'RIN'},inplace=True)
        fresh_engagement_dates.reset_index(inplace=True,drop=False)
        fresh_engagement_dates['PatientID'] = fresh_engagement_dates['PatientID'].astype('str')
        fresh_engagement_dates = fresh_engagement_dates[['PatientID','RIN','EngagementDate']]

        toSQL.toSQL(fresh_engagement_dates,exist_method='append',table='rpt_engagement_date')
