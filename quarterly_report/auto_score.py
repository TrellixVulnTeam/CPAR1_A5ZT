import pandas as pd
from quarterly_report import tier_scores
from conconnect.conconnect import ConsensusConnect
import datetime

class AutoScore():
    def __init__(self,assessment_data):
        self.assessDict = {
            'PROMIS':{'PROMIS_baseline':['Tier 2 PROMIS Tool',None],
                      'PROMIS_6mo':['Tier 2 PROMIS Tool - 6 Month',None],
                      'PROMIS_12mo':['Tier 2 PROMIS Tool - 12 Month',None],
                      'PROMIS_18mo':['Tier 2 PROMIS Tool - 18 Month',None]},
            'PSC':{'PSC17_baseline':['Tier 2 Pediatric Symptom Checklist (PSC-17) Caregiver',None],
                   'PSC17_6mo':['Tier 2 Pediatric Symptom Checklist (PSC-17) Caregiver - 6 Month',None],
                   'PSC17_12mo':['Tier 2 Pediatric Symptom Checklist (PSC-17) Caregiver - 12 Month',None]},
            'PHQA':{'PHQA_baseline':['Tier 3 PHQ-A Caregiver Report',None],
                    'PHQA_6mo':['Tier 3 PHQ-A Caregiver Report - 6 Month',None],
                    'PHQA_12mo':['Tier 3 PHQ-A Caregiver Report - 12 Month',None]},
            'PHQ9':{'PHQ9_baseline':['Tier 3 PHQ9 Adult or Caregiver',None],
                    'PHQ9_6mo':['Tier 3 PHQ9 Adult or Caregiver - 6 Month',None],
                    'PHQ9_12mo':['Tier 3 PHQ9 Adult or Caregiver - 12 Month',None]},
            'ACT':{'ACT_baseline':['Tier 2 Asthma Control Test (ACT)',None]},
            'Edinburgh':{'EDI_baseline':['Tier 2 Edinburgh (Post-Partum Depression)',None]},
            'MHSPatient':{'MHSPatient_baseline':['Tier 3 Patient Mental Health Screening',None]},
            'MHSParent':{'MHSParent_baseline':['Tier 3 Parent Mental Health Screening',None]},
            'CHAOS':{'CHAOS_baseline':['Tier 2 PROMIS Tool',None],
                     'CHAOS_6mo':['Tier 2 PROMIS Tool - 6 Month',None]}
            }
        self.assessment_data = assessment_data
        self.assessment_data.AssessmentName = self.assessment_data.AssessmentName.str.strip()
    def __call__(self):
        for assessment in self.assessDict:
            print(assessment)
            for subassessment in self.assessDict[assessment]:
                self.assessDict[assessment][subassessment][1] = self.assessAuto(self.assessDict[assessment][subassessment][0],assessment)

        self.assessDict['Total'] = self.assessmentTotals()
        print('Total')
        return self.assessDict

    def assessmentPivot(self,tier_data,assessmentName):
        '''Tier data is the query pulled from the consensus database, assessmentName is assessment to select'''
        assessment_data = tier_data.loc[tier_data.AssessmentName==assessmentName,:]
        assessment_pivot = assessment_data.pivot(index='PatientID',columns='QuestionText',values='Range')
        return assessment_pivot, assessment_data

    def assessAuto(self,assessment,subAssess):
        rawAssessData, assessment_data = self.assessmentPivot(self.assessment_data,assessment)
        preScoredAssessData = tier_scores.TierScores(rawAssessData)
        if subAssess == 'PROMIS':
            scoredAssessData = preScoredAssessData.PROMIS()
        elif subAssess == 'PSC':
            scoredAssessData = preScoredAssessData.PSC()
        elif subAssess == 'PHQA':
            scoredAssessData = preScoredAssessData.PHQA()
        elif subAssess == 'PHQ9':
            scoredAssessData = preScoredAssessData.PHQ9()
        elif subAssess == 'ACT':
            # ACT does not work like any other scored assessments as it gets over ridden the data is made
            # by a stored procedure in my sql call rpt_act_scores
            connection = ConsensusConnect()
            scoredAssessData = connection.actScore()
            scoredAssessData['AssessmentName'] = assessment
            print("ACT updated {}".format(scoredAssessData['cdate'][0]))
            return scoredAssessData
        elif subAssess == 'Edinburgh':
            scoredAssessData = preScoredAssessData.Edinburgh()
        elif subAssess == 'MHSPatient':
            scoredAssessData = preScoredAssessData.MHSPatient()
        elif subAssess == 'MHSParent':
            scoredAssessData = preScoredAssessData.MHSParent()
        elif subAssess == 'CHAOS':
            scoredAssessData = preScoredAssessData.CHAOS()
        #gives the assessment name to df and then merges to get start date of assessment. Can be used to append any column
        scoredAssessData['AssessmentName'] = assessment
        dateFrame = assessment_data[['PatientID','StartDate','CUserID']]
        dateFrame.drop_duplicates(inplace=True)
        scoredAssessData = scoredAssessData.reset_index()
        scoredAssessData = pd.merge(scoredAssessData,dateFrame,on='PatientID')
        #for some damn reason a patient can take the assessment more than once...
        scoredAssessData.sort_values(by=['PatientID','StartDate'], inplace=True)
        scoredAssessData.drop_duplicates(subset=['PatientID'], inplace=True)
        scoredAssessData.set_index(["PatientID"],inplace=True)
        return scoredAssessData[list(scoredAssessData.columns[-2:])+list(scoredAssessData.columns[:-2])]

    def renameColsDic(self,lst,assessment_name,str_append):
        '''Renames columns for assessmentTotals'''
        rename_col = {}
        for col in lst:
            if col.startswith(assessment_name):
                rename_col[col] = "_".join([col,str_append])
            else:
                rename_col[col] = "_".join([assessment_name,col,str_append])
        return rename_col

    def assessmentTotals(self):
        '''Creates a pivot table for the scored assessments and stores all of the final scores of the
        assessments into one dataframe. This does NOT include ACT because ACT is so special'''

        PROMIS_cols = ['Anxiety T-Score','Depression T-Score','Emotional T-Score',
                       'Informational T-Score','Instrumental T-Score','Social T-Score']

        col_names = ['AssessmentName','Sequence','Score_Type','StartDate','Total Score']

        total_score_df = pd.DataFrame(columns=col_names)
        total_score_df.index.rename('PatientID',inplace=True)

        for assessment_name in self.assessDict:
            if assessment_name == 'ACT':
                continue
            else:
                for sub_assess in self.assessDict[assessment_name]:
                    assessment = self.assessDict[assessment_name][sub_assess][1].copy()
                    assessment['AssessmentName'] = assessment_name
                    sequence = sub_assess.split('_')[1].capitalize()
                    assessment['Sequence'] = sequence
                    if assessment_name == 'PROMIS':
                        for i in PROMIS_cols:
                            assessment['Score_Type'] = i
                            assessment['Total Score'] = assessment[i]
                            total_score_df = total_score_df.append(assessment[col_names])
                    else:
                        assessment['Score_Type'] = 'Total Score'
                        total_score_df = total_score_df.append(assessment[col_names])
        total_score_df.reset_index(inplace=True)
        pivot_total_score_df = total_score_df.pivot_table(index='PatientID',
                                                          columns=['AssessmentName','Score_Type','Sequence'],
                                                          values=['Total Score','StartDate'],aggfunc='first')

        return pivot_total_score_df
