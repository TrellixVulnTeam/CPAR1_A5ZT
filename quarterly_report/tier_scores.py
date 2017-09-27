import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

class TierScores:
    '''Calculates assessment scores for patients. This class requires a data frame to initialize, dataframe should be
    the consensus database.'''

    def __init__(self,df):
        #Converts column names and variables
        self.df = df
        #all assessments will have these columns
        self.dataDic = {
            'PROMIS':{
                'Anxiety':{
                    'Clean' : ['1. I felt fearful... ','2. I found it hard to focus on anything other than my anxiety...',
                               '3. My worries overwhelmed me...','4. I felt uneasy...'],
                    'ScoreCol' : 'Anxiety Score'},
                'Depression':{
                    'Clean' : ['1. I felt worthless...','2. I felt helpless...','3. I felt depressed...','4. I felt hopeless...'],
                    'ScoreCol' : 'Depression Score'},
                'Emotional':{
                    'Clean' : ['1. I have someone who will listen to me when I need to talk','2. I have someone to confide in or talk to about myself or my problems',
                               '3. I have someone who makes me feel appreciated','4. I have someone to talk with when I have a bad day'],
                    'ScoreCol':'Emotional Score'},
                'Informational':{
                    'Clean' : ['1. I have someone to give me good advice about a crisis if I need it','2. I have someone to turn to for suggestions about how to deal with a problem',
                               '3. I have someone to give me information if I need it','4. I get useful advice about important things in my life'],
                    'ScoreCol':'Informational Score'},
                'Instrumental':{
                    'Clean' : ['1. Do you have someone to help you if you are confined to bed?','2. Do you have someone to take you to the doctor if you need it?',
                               '3. Do you have someone to help you with your daily chores if you are sick?','4. Do you have someone to run errands if you need it?'],
                    'ScoreCol':'Instrumental Score'},
                'Social':{
                    'Clean' : ['1. I have trouble doing all my regular leisure activities with others','2. I have trouble doing all the family activities that I want to',
                               '3. I have trouble doing all my usual work (include work at home)','4. I have trouble doing all the activities with friends that I want to do'],
                    'ScoreCol':'Social Roles Score'}},
            'CHAOS':{
                'Clean' : ['1. I have a regular morning routine',"2. You can't hear yourself think in our home",
                           "3. It's a real zoo in our home",'4. We are usually able to stay on top of things',
                           '5. There is usually a television turned on somewhere in our home','6. The atmosphere in our house is calm ']},
            'ACT':{
                'Clean' : ["1. In the past 4 weeks, how much time did your/your child\'s asthma keep you/him or her from getting as much done at work, school or home?",
                           '2. During the past 4 weeks, how often have you/your child had shortness of breath?',
                           "3. During the past 4 weeks, how often did you/your child's asthma symptoms (wheezing, coughing, shortness of breath, chest tightness or pain) wake you/your child up at night or earlier than usual in the morning?",
                           "4. During the past 4 weeks, how often have you/your child used your/your child's rescue inhaler or nebulizer machine (such as albuterol)?",
                           "5. How would you rate your/your child's asthma control during the past 4 weeks?"]},
            'Edinburgh':{
                'Clean': ['1. I have been able to laugh and see the funny side of things','2. I have looked forward with enjoyment to things',
                          '3. I have blamed myself unnecessarily when things went wrong','4. I have been anxious or worried for no good reason',
                          '5. I have felt scared or panicky for no very good reason','6. Things have been getting on top of me',
                          '7. I have been so unhappy that I have had difficulty sleeping','8. I have felt sad or miserable',
                          '9. I have been so unhappy that I have been crying','10. The thought of harming myself has occurred to me']},
            'PSC':{
                'Attention Problems':{
                    'Clean' : ['6. Fidgety, unable to sit still','10. Acts as if driven by a motor','7. Daydreams too much',
                               '8. Distracted easily','9. Has trouble concentrating'],
                    'ScoreCol' : 'Attention Problems'},
                'Internalizing Problems':{
                    'Clean' : ['1. Feels sad, unhappy','2. Feels hopeless','3. Is down on self','4. Worries a lot',
                               '5. Seems to be having less fun'],
                    'ScoreCol' : 'Internalizing Problems'},
                'Externalizing Problems':{
                    'Clean' : ['11. Fights with other children','12. Does not listen to rules',"13. Does not understand other people's feelings",
                               '14. Teases others','15. Blames others for his/her troubles','16. Refuses to share',
                               '17. Takes things that do not belong to him/her'],
                    'ScoreCol' : 'Externalizing Problems'}},
            'PHQ9':{
                'Clean' : ['1. Little interest or pleasure in doing things?',
                #'10. If you checked off any problems, how difficult have these problems made it for you to do your work, take care of things at home, or get along with other people?',
                           '2. Feeling down, depressed or hopeless?','3. Trouble falling asleep, staying asleep, or sleeping too much? [PHQ9]',
                           '4. Feeling tired, or having little energy?','5. Poor appetite or overeating?','6. Feeling bad about yourself - or feeling that you are a failure, or that you have let yourself or your family down?',
                           '7. Trouble concentrating on things, such as reading the newspaper or watching television?',
                           '8. Moving or speaking so slowly that other people could have noticed? Or the opposite - being so fidgety or restless that you have been moving around a lot more than usual? [PHQ9]',
                           '9. Thoughts that you would be better off dead, or of hurting yourself in some way?']},
            'PHQA':{
                'Clean' : ['1. Feeling down, depressed, irritable or hopeless?','2. Little interest or pleasure in doing things?',
                           '3. Trouble falling asleep, staying asleep, or sleeping too much?','4. Poor appetite, weight loss or overeating?',
                           '5. Feeling tired, or having little energy?','6. Feeling bad about yourself - or feeling that you are a failure, or that you have let yourself or your family down?',
                           '7. Trouble concentrating on things like school work, reading or watching TV?','8. Moving or speaking so slowly that other people could have noticed? OR...Being so fidgety or restless that you were moving around a lot more than usual?',
                           '9. Thoughts that you would be better off dead, or of hurting yourself in some way?','10. Has there been a time in the past month when you have had serious thoughts of ending your life?',
                           '11. Have you EVER, in your whole life, tried to kill yourself or made a suicide attempt?']},
            'MHSParent':{
                'Clean' : ['1. Are you actively receiving treatment from a mental health professional currently?',
                           "2. Do you have symptoms of mania (feeling high or very happy) or psychosis (strange experiences like hearing things or seeing things that other people didn't)?",
                           '3. Have you ever been diagnosed with MDD or Bipolar disorder?',
                           '4. Do you have a drug or alcohol problem? ']},
            'MHSPatient':{
                'Clean' : ['1. Do you/your child have a disability or autism?',
                           '2. Are you/Is your child actively receiving treatment from a mental health professional currently?',
                           "3. Do you/ your child have symptoms of mania (feeling high or very happy) or psychosis (strange experiences like hearing things or seeing things that other people didn't)?",
                           '4. Have you/your child ever been diagnosed with MDD or Bipolar disorder?',
                           '5. Do you/your child have a drug or alcohol problem? ']}}

    def PHQscale(self,x):
            if x <= 4:
                x = "No Signs of Depression"
            elif x<=9:
                x = 'Mild Depression'
            elif x<=14:
                x = 'Moderate Depression'
            elif x<=19:
                x = 'Moderately Severe Depression'
            elif x<=27:
                x = 'Severe Depression'
            return x

    def frameMaker(self,i):
        '''Makes a new frame with record ID, Pt ID and the CHW that performed assesment from original DF'''
        newFrame = pd.DataFrame()
        return newFrame

    def scoreSum(self,frame,cleanCol,sumCol,skipNA=True):
        '''Changes the original column names to another dataframe and then sums the columns'''
        #Changes column names and then sums across the columns
        frame[cleanCol] = self.df[cleanCol]
        frame.dropna(how='all',subset=cleanCol,inplace=True)
        frame[sumCol] = frame[cleanCol].sum(axis=1,skipna=skipNA)
        return frame

    def newFrameSum(self,i):
        ##Manager method of frameMaker and scoreSum
        self.newFrame = self.frameMaker(i)
        self.scoreSum(self.newFrame,self.dataDic[i]['Clean'],'Total Score')
        return self.newFrame

    def PSC(self,onlyScores=None):
        '''Pediatric survey that is designed to facilitate the recognition of emotional, cognitive and behavioral problems.
        Specific questions are aggregated to scores. This goes to Externalizing, Internalizing, and Attentional problems.
        The scores are the aggregated into a single column called PSC Total Score'''

        pscFrame = self.frameMaker('PSC')
        #iterates through the individual assessments in the dataDic
        for i in self.dataDic['PSC']:
            self.scoreSum(pscFrame,self.dataDic['PSC'][i]['Clean'],
                          self.dataDic['PSC'][i]['ScoreCol'])

        pscFrame['Total Score'] = (pscFrame['Externalizing Problems'] +
                                            pscFrame['Internalizing Problems'] +
                                            pscFrame['Attention Problems'])
        #Creates categorical columns
        pscFrame['Attention Problems At Risk? (Score Q(6-10) > 7)'] = pscFrame['Attention Problems'] >= 7
        pscFrame['Internalizing Problems At Risk? (Score Q(1-5) > 5)'] = pscFrame['Internalizing Problems'] >= 5
        pscFrame['Externalizing Problems At Risk? (Score Q(11-17) > 7)'] = pscFrame['Externalizing Problems'] >= 7
        pscFrame['PSC Total Score >= 15'] = pscFrame['Total Score'] >= 15
        return pscFrame

    def Edinburgh(self,onlyScores=None):
        '''Edinburgh Score tests for post-partum depression.'''
        ediFrame = self.newFrameSum('Edinburgh')
        def ediScale(x):
            if x <= 8:
                x = 'Low probability of depression'
            elif x <= 12:
                x = 'Most likely just dealing w/ a new baby or the baby blues.'
            elif x <= 14:
                x = 'Signs leading to possibility of PPD; take preventative measures.'
            else:
                x = 'High probability of experiencing clinical depression.'
            return x
        ediFrame['Edi-Scale'] = ediFrame['Total Score'].apply(ediScale)
        if onlyScores == True:
            return ediFrame[['Record ID','Patient ID','CHW', 'Total Score','Edi-Scale']]
        return ediFrame

    def ACT(self,onlyScores=None):
        '''Asthma assessment scores'''
        ACTframe = self.newFrameSum('ACT')
        def actScale(x):
            if x <= 19:
                x = 'Pt\'s asthma symptoms may not be in as much control as they should be '
            else:
                x = 'Pt appears to have well controlled asthma.'
            return x
        ACTframe['ACT result'] = ACTframe['ACT Total Score'].apply(actScale)
        return ACTframe

    def CHAOS(self,onlyScores=None):
        '''Chaos scoring: drops any that has zero on sum.'''
        CHAOS = self.newFrameSum('CHAOS')
        CHAOS = CHAOS.loc[CHAOS['Total Score']!=0,:]
        if onlyScores == True:
            return CHAOS[['Record ID','Patient ID','CHW','Total Score']]
        else:
            return CHAOS

    def PROMIS(self,onlyScores=None):
        Promis = self.frameMaker('PROMIS')
        'iterates through the data dic to pull columns of data, sum then sums'
        for i in self.dataDic['PROMIS']:
            self.scoreSum(Promis,self.dataDic['PROMIS'][i]['Clean'],
                          self.dataDic['PROMIS'][i]['ScoreCol'],skipNA=False)
        promisAnxScale = {4:40.3,5:48,6:51.2,7:53.7,8:55.8,9:57.7,10:59.5,11:61.4,12:63.4,
                          13:65.3,14:67.3,15:69.3,16:71.2,17:73.3,18:75.4,19:77.9,20:81.6}
        promisDepScale = {4:41.0,5:49.0,6:51.8,7:53.9,8:55.7,9:57.3,10:58.9,11:60.5,12:62.2,
                          13:63.9,14:65.7,15:67.5,16:69.4,17:71.2,18:73.3,19:75.7,20:79.4}
        promisEmoScale = {4:25.7,5:29.9,6:32.1,7:34.0,8:35.7,9:37.3,10:38.9,11:40.5,12:42.1,
                          13:43.7,14:45.4,15:47.2,16:49.0,17:50.8,18:53.0,19:55.6,20:62.0}
        promisInfoScale = {4:25.6,5:29.8,6:32.2,7:34.2,8:36.1,9:37.9,10:39.8,11:41.8,12:43.9,
                          13:46.0,14:48.1,15:50.3,16:52.4,17:54.7,18:57.1,19:60.1,20:65.6}
        promisInstScale = {4:29.3,5:33.9,6:35.9,7:37.6,8:39.1,9:40.5,10:41.8,11:43.1,12:44.5,
                          13:45.9,14:47.3,15:48.9,16:50.5,17:52.3,18:54.4,19:57.1,20:63.3}
        promisSocScale = {4:27.5,5:31.8,6:34.0,7:35.7,8:37.3,9:38.8,10:40.5,11:42.3,12:44.2,
                          13:46.2,14:48.1,15:50.0,16:51.9,17:53.7,18:55.8,19:58.3,20:64.2}
        Promis['Anxiety T-Score'] = Promis['Anxiety Score'].map(promisAnxScale)
        Promis['Depression T-Score'] = Promis['Depression Score'].map(promisDepScale)
        Promis['Emotional T-Score'] = Promis['Emotional Score'].map(promisEmoScale)
        Promis['Informational T-Score'] = Promis['Informational Score'].map(promisInfoScale)
        Promis['Instrumental T-Score'] = Promis['Instrumental Score'].map(promisInstScale)
        Promis['Social T-Score'] = Promis['Social Roles Score'].map(promisSocScale)
        return Promis

    def MHSParent(self, onlyScores=None):
        MHSParent = self.newFrameSum('MHSParent')
        if onlyScores == True:
            return MHSParent[['Record ID','Patient ID','CHW','Total Score']]
        return MHSParent

    def MHSPatient(self, onlyScores=None):
        MHSPatient = self.newFrameSum('MHSPatient')
        if onlyScores == True:
            return MHSPatient[['Record ID','Patient ID','CHW','Total Score']]
        return MHSPatient

    def PHQ9(self, onlyScores=None):
        PHQ9 = self.newFrameSum('PHQ9')
        PHQ9['PHQ9 Result'] = PHQ9['Total Score'].apply(self.PHQscale)
        return PHQ9

    def PHQA(self,onlyScores=None):
        PHQA = self.newFrameSum('PHQA')
        PHQA['PHQA Result'] = PHQA['Total Score'].apply(self.PHQscale)
        return PHQA
