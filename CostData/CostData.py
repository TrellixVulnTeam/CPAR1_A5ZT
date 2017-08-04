import pandas as pd
import datetime
import shutil
import os

class CostData(object):
    '''To run this class the interpreter should be within the directory which has the original cost file data. A directory that contains only the Category mapping should be in the same level as the original files. To initiate all cost data should be within the Cost Data file. A new directory will be created, this new directory will contain the file with an updated category column. These new files are then grouped; see the groupDic for the specific categories called.

    --Cost Data
        --Cost Data for Month
            --Category Directory
                --Category for quarterly report.xlsx
            --cost data1.csv
            --cost data2.csv
            --updateFile-MM-DD-YYYY <-This will be created.
                --updatedFile
                --groupedFile

    Lastly sit back enjoy.
    '''

    def __init__(self):
        '''__init__ lists all files cost data, and also '''
        #opens all files
        self.costFiles = os.listdir()
        #this directory contains the mapping file
        os.chdir('Category Directory')
        oslist = os.listdir()
        self.categoryFile = self.colStripper(pd.read_excel(oslist[0]))
        os.chdir('..')
        self.updateFile = 'updateFile-{:%m-%d-%Y}'.format(datetime.date.today())

    def __call__(self):
        '''Allows easy call '''
        self.updateDF()
        self.groupDF()
        os.chdir('..')

    def dataFrameDic(self,directory):
        '''reads all csv and xlsx files in a directory and puts them into a dictionary'''
        DFDic = {}
        for file in directory:
            if '.xlsx' in file:
                df = pd.read_excel(file)
            elif '.csv' in file:
                df = pd.read_csv(file)
            else:
                continue
            DFDic[file] = self.colStripper(df)
        return DFDic

    def colStripper(self, df):
        '''Selects all columns which contain type str in a DF and removes extraneous whitespace'''
        for column in list(df.select_dtypes(include=['O']).columns):
            df[column] = df[column].str.strip()
        return df

    def datemaker(self,string):
        return '01-'+ string

    def updateDF(self):
        '''Makes a directory that will store the ouput files with the new categories column'''
        while True:
            try:
                os.mkdir(self.updateFile)
                break
            except FileExistsError:
                shutil.rmtree(self.updateFile)

        DFDic = self.dataFrameDic(self.costFiles)
        #Creates a mapping file to change group the files
        categoryMapping = dict(zip(self.categoryFile['Old Category'],self.categoryFile['New']))

        for fileName in DFDic:
            original_DF_File = DFDic[fileName]
            original_DF_File['Date'] = original_DF_File['Date'].apply(self.datemaker)
            original_DF_File['Date'] = pd.to_datetime(original_DF_File['Date'])

            if 'Categories' in list(original_DF_File.columns):
                original_DF_File['CHECK Categories'] = original_DF_File['Categories'].map(categoryMapping)
            elif 'Service Category' in list(original_DF_File.columns):
                original_DF_File['CHECK Categories'] = original_DF_File['Service Category'].map(categoryMapping)
            else:
                continue

            #reorder columns
            columnList = list(original_DF_File.columns)
            original_DF_File = original_DF_File[columnList[0:2] + [columnList[-1]] + columnList[2:-1]]
            newFile = 'Updated_' + fileName[:-4] + '.xlsx'
            os.chdir(self.updateFile)
            original_DF_File.to_excel(newFile, index = False)
            os.chdir('..')
        return print('New Categories Added to DF')

    def groupDF(self):
        '''Iterates through the updated files and groups the files according to
        their specific grouping  which is stored in the groupDic.'''

        os.chdir(self.updateFile)
        groupDic = {'diagnosisGroup':['Date','CHECK Categories','Disease','Patient Cohort'],
        'ageGroup':['Date','CHECK Categories','Age Category','Patient Cohort'],
        'riskGroup':['Date','CHECK Categories','Risk Levels','Patient Cohort'],
        'encounterGroup':['Date','CHECK Categories','Patient Cohort']}

        groupDFDic = self.dataFrameDic(os.listdir())

        for File in groupDFDic:
            groupedDF = None
            if 'Age_Report' in File:
                groupedDF = groupDFDic[File].groupby(by=groupDic['ageGroup']).sum().reset_index()
            elif 'Risk_Report' in File:
                groupedDF = groupDFDic[File].groupby(by=groupDic['riskGroup']).sum().reset_index()
            elif 'Diagnosis_Report' in File:
                groupedDF = groupDFDic[File].groupby(by=groupDic['diagnosisGroup']).sum().reset_index()
            elif 'Encounter_Summary_Report' in File:
                groupedDF = groupDFDic[File].groupby(by=groupDic['encounterGroup']).sum().reset_index()
            groupedDF.to_excel('Group_'+File[8:-4]+'xlsx', index = False)

        return print('Completed grouping!!')
