import datetime
import os
import shutil
import pandas as pd
from CHECK.conconnect import conconnect
from CHECK.helpers import reportHelper
from CHECK.helpers import contactHelper

class monthlyReports():

    def __init__(self):
        #creates a directory which the reports will go into
        self.connection = conconnect.ConsensusConnect()
        os.chdir("/Users/gmunoz/Projects/Reports/Output_Data/")
        fileName = '{:%Y-%m-%d}'.format(datetime.date.today())
        if os.path.exists(fileName):
            shutil.rmtree(fileName)
        os.mkdir(fileName)
        os.chdir(fileName)
        self.demography = None
        self.patCareTeam = None


    def actReport(self,start_date,end_date):
        # start date and end date need following format yyyy-mm-dd
        act_df = self.connection.actScore()
        pat_care_team = self.dataHolder('patCareTeam')

        # print("ACT last updated {}, call act_temp() in mysql for more recent ".format(act_df['cdate'][0].date()))
        act_df['StartDate'] = pd.to_datetime(act_df['StartDate'])
        trun_act_df = act_df.loc[act_df['StartDate'].between(start_date,end_date)]
        trun_act_df = trun_act_df[['PatientID','StartDate','Assessment_Count','ACT_Total_Score','ACT_Result']]
        trun_act_df = pd.merge(trun_act_df,pat_care_team,on='PatientID',how='left')

        date = datetime.date.today()
        my_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        month = my_date.strftime("%b")
        file_name = "ACT_{}_Report_".format(month)
        self.printFileOutput(trun_act_df,file_name)

    def activeEngagedBCBS(self):
        '''Redone'''

        demography = self.dataHolder('demography')
        demography['Risk'] = demography['Consensus_Risk']

        bcbs_pats = demography.loc[(demography['MCO_Name']=='Blue Cross Blue Shield')&
                                   (demography['Current_Enrollment_Status']=='Active')&
                                   (demography['Group']=='Engaged')]

        bcbs_pats = bcbs_pats[['PatientID','RecipientID','First_Name','Last_Name',
                               'Gender','DOB','Current_Enrollment_Status']]

        self.printFileOutput(bcbs_redcap,"Active_Engaged_BCBS_Report")

    def enrolledNotEngaged(self):
        '''Redone'''

        pat_care_team = self.dataHolder('patCareTeam')
        demography = self.dataHolder('demography')

        active_enrolled = demography.loc[(demography['Current_Enrollment_Status']=='Active')&
                                                 (demography['Group']=='Enrolled')]

        active_enrolled_assigned = pd.merge(active_enrolled,pat_care_team,how='left',on='PatientID')
        active_enrolled_assigned['AssignedTo'] = active_enrolled_assigned['AssignedTo'].fillna("Unassigned")
        active_enrolled_assigned = active_enrolled_assigned[['PatientID','RecipientID','AssignedTo','Initial_Enrollment_Date']]
        self.printFileOutput(active_enrolled_assigned,"Enrolled_Not_Engaged_Report")

    def careplanMonthlyReports(self):

        chw_mapping = self.connection.chwmapping()
        chw_mapping = chw_mapping[['PatientID','NetID']]

        chw_df = self.connection.chwquery()
        chw_df = chw_df[['ConsensusUsername','FirstName','NetID']]
        chw_mapping = pd.merge(chw_mapping,chw_df,how='left',on='NetID')

        demography = self.dataHolder('demography')
        pat_care_team = self.dataHolder('patCareTeam')

        demography['Risk'] = demography['Consensus_Risk']

        contact = self.connection.contact()
        careplan = self.connection.careplan()

        contact_group_piv = contactHelper.pivotRecentContact(contact)
        contact_group_piv.reset_index(inplace=True)
        contact_group_piv.sort_values('PatientID',inplace=True)
        active_engaged_demo = demography.loc[(demography['Status']=='Active')]

        active_engaged_demo = active_engaged_demo[['PatientID','Patient_Type','EngagementDate','Risk']]
        active_engaged_contact_demo = pd.merge(active_engaged_demo,contact_group_piv,on='PatientID',how='left')
        active_engaged_contact_demo = pd.merge(active_engaged_contact_demo,demography[['PatientID','RIN']],on='PatientID',how='left')

        careplan_mapping = pd.merge(active_engaged_contact_demo,careplan,on=['PatientID','RIN'],how='left')
        careplan_mapping = pd.merge(careplan_mapping,chw_mapping,how='left')

        engaged_no_careplan = careplan_mapping.loc[(careplan_mapping['StartDate'].isnull())&
                                                   (careplan_mapping['Patient_Type']=='Engaged')]

        engaged_careplan = careplan_mapping.loc[(~careplan_mapping['StartDate'].isnull())&
                                                (careplan_mapping['Patient_Type']=='Engaged')]

        engaged_no_careplan = pd.merge(engaged_no_careplan,pat_care_team,on='PatientID',how='left')
        engaged_careplan = pd.merge(engaged_careplan,pat_care_team,on='PatientID',how='left')
        engaged_no_careplan = engaged_no_careplan[['PatientID','RIN','Patient_Type','EngagementDate',
                                                   'Risk','NetID','ConsensusUsername','FirstName','AssignedTo']]

        engaged_no_careplan['AssignedTo'] = engaged_no_careplan['AssignedTo'].fillna('Unassigned')

        self.printFileOutput(engaged_no_careplan,"Engaged_no_careplan")
        self.printFileOutput(engaged_careplan,"Engaged_careplan")

    def activeMCO(self):
        '''Redone'''
        pat_care_team = self.dataHolder('patCareTeam')
        demography = self.dataHolder('demography')
        active_demo = demography.loc[demography['Current_Enrollment_Status']=='Active',
                                    ['PatientID','Current_Enrollment_Status',
                                    'RecipientID','MCO_Name','Group']]
        active_demo = pd.merge(active_demo,pat_care_team,on='PatientID',how='left')
        active_demo = active_demo[['PatientID','Current_Enrollment_Status','MCO_Name']]
        self.printFileOutput(active_demo,"Patient_CHW_MCO")

    def runAll(self):
        start_date = input("Start Date for ACT Report (yyyy-mm-dd): ")
        end_date = input("End Date for ACT Report (yyyy-mm-dd): ")
        self.actReport(start_date,end_date)
        self.activeEngagedBCBS()
        self.enrolledNotEngaged()
        self.careplanMonthlyReports()
        self.activeMCO()

    def dataHolder(self,query):
        '''Intakes a query name for conconnect, the goal is to save time on queries by storing
        the query output to the object allowing for access if a different report uses the same
        query'''

        if query == 'demography':
            if self.demography is None:
                self.demography = self.connection.totalDemo()
            return self.demography.copy()
        elif query == 'patCareTeam':
            if self.patCareTeam is None:
                self.pat_care_team = self.connection.patCareTeam()
            return self.pat_care_team.copy()

    def printFileOutput(self,df,file_name):
        file_name = reportHelper.fileNameDate(file_name)
        df.to_excel(file_name,index=False)
        print("File Name: {}".format(file_name))
