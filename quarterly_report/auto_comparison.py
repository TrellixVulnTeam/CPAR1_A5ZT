import pandas as pd

class autocompare():
    def __init__(self,assessment):

    def assessmentComparison(assessmentDF,assessmentName):

    def seDiff(self,std1,std2,n1,n2):
        '''Standard Error'''
        return np.sqrt((np.square(std1)/n1)+(np.square(std2)/n2))
    def assessmentStats(df,assessment,col1,col2):
        df['Assessment'] = assessment
        df_pivot = df.groupby('Assessment').agg([np.mean,np.std,scipy.stats.sem])
        assess_pscore = stats.ttest_rel(df[col1],df[col2])
        n = len(df)
        df_pivot['n'] = n
        df_pivot['p-value'] = assess_pscore[1]
        df_pivot['SE_diff'] = seDiff(df_pivot[(col1,'std')],df_pivot[(col2,'std')],n,n)
        df_pivot['95% CI'] = df_pivot['SE_diff']*1.98
        df['Diff'] = df_pivot[(col2,'mean')] - df_pivot[(col1,'mean')]
        return df_pivot[['n',col1,col2,'p-value','SE_diff','95% CI']]
