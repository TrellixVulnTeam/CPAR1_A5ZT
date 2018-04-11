import pandas as pd
import numpy as np
from datetime import timedelta, date
from CHECK.dbconnect import dbconnect

class RiskCategorizer(object):

    def risk_tier_calc(self, pat_df_in, risk_col_name, risk_col_values):
        '''calculates risk for 12 the months from '''
        pat_df_in.loc[(pat_df_in['ED'] > 3) | (pat_df_in['IP'] > 1),
                      risk_col_name] = risk_col_values[2]
        pat_df_in.loc[((pat_df_in['ED'] <= 3) & (pat_df_in['ED'] >= 1)) |
                      (pat_df_in['IP'] == 1), risk_col_name] = risk_col_values[1]
        pat_df_in.loc[(pat_df_in['ED'] == 0) & (pat_df_in['IP'] == 0),
                      risk_col_name] = risk_col_values[0]
        return pat_df_in

    def risk_window(self, pat_df_in, ed_ip_df, date_col, risk_col_name,
                    risk_col_values, pat_info_flag=True):
        '''Selects IP and ED bills that occured between the date_col and
            12 months back pat_df_in: pandas dataframe w/ columns ED, IP,
            ServiceFromDt, and date_col ed_ip_df: pandas dataframe'''
        window_size = 12
        pat_df = pat_df_in.copy()
        pat_df['Low_Window_Date'] = pat_df[date_col].apply(lambda x:
                                                           x - pd.DateOffset
                                                           (months=window_size)
                                                           )
        if pat_info_flag:
            pat_df = pd.merge(pat_df, ed_ip_df, on='RecipientID', how='left')
        pat_df['ServiceFromDt'] = pd.to_datetime(pat_df['ServiceFromDt'])
        pat_df[date_col] = pd.to_datetime(pat_df[date_col])
        pat_df = pat_df.loc[pat_df['ServiceFromDt']
                            .between(pat_df['Low_Window_Date'],
                                     pat_df[date_col])]
        pat_df = pat_df.groupby(['RecipientID'],
                                as_index=False).agg({'ED': np.sum,
                                                     'IP': np.sum})
        if pat_info_flag:
            pat_df = pd.merge(pat_df_in[['RecipientID']], pat_df,
                              on='RecipientID', how='left')

        pat_df.fillna(0, inplace=True)
        pat_df = self.risk_tier_calc(pat_df, risk_col_name, risk_col_values)
        pat_df.set_index('RecipientID', inplace=True)
        return pat_df

    def ip_ed_query(self):
        ip_ed_df = self.connector.query('''SELECT RecipientID,ServiceFromDt,
                                      'IP' AS Category,COUNT(1) AS encounters
                                      FROM
                                          tsc_hfs_main_claims_new
                                      WHERE
                                           Category3 IN ('INPATIENT_IP' ,
                                           'EMERGENCY_IP')
                                            AND RejectionStatusCd = 'N'
                                      GROUP BY RecipientID , ServiceFromDt
                                      UNION SELECT
                                          RecipientID,ServiceFromDt,
                                          'ED' AS Category,COUNT(1)
                                          AS encounters
                                      FROM
                                          tsc_hfs_main_claims_new
                                      WHERE
                                          Category3 IN ('EMERGENCY_OP')
                                              AND RejectionStatusCd = 'N'
                                      GROUP BY RecipientID , ServiceFromDt;''')
        return ip_ed_df

    def main(self, release_num, release_date):

        self.connector = dbconnect.DatabaseConnect('CHECK_CPAR2')

        self.max_date = pd.Timestamp(release_date)
        self.ip_ed_df =  self.ip_ed_query()

        self.ip_ed_df = pd.pivot_table(self.ip_ed_df, index=['RecipientID',
                                  'ServiceFromDt'], columns='Category',
                                  values='encounters', aggfunc='first',
                                  fill_value=0)
        self.ip_ed_df.reset_index(inplace=True)

        self.enroll_df = self.connector.query('''select RecipientID,
                                       if(Initial_Enrollment_Date is null,
                                       Program_date, Initial_Enrollment_Date)
                                       as Initial_Enrollment_Date from
                                       pat_info_demo;''')
        self.engage_df = self.connector.query('''select RecipientID,
                                       Engagement_Date from pat_info_demo WHERE
                                       Engagement_Date is not null;''')

    def all_release_risk_cal(self):

        all_release_df = self.connector.query('''SELECT ReleaseNum,
                                                 HFS_Release_Date
                                                 FROM hfs_release_info;''')
        self.risk_col_values = ['2' ,'5', '7']

        self.ip_ed_df = pd.merge(self.enroll_df, self.ip_ed_df,
                                 on='RecipientID', how='left')

        self.ip_ed_df.reset_index(inplace=True)
        self.all_release_risk_df = self.enroll_df[['RecipientID']]

        all_release_df.apply(self.calAllRisk, axis = 1)
        self.all_release_risk_df.fillna(0, inplace=True)

        self.all_release_risk_df.iloc[:,1:] = self.all_release_risk_df.iloc[:,1:].astype(int)
        self.all_release_risk_df['Total'] = self.all_release_risk_df.iloc[:,1:].sum(axis = 1)
        self.all_release_risk_df['Pattern'] = self.all_release_risk_df.iloc[:,1:len(self.all_release_risk_df.columns)-2].apply(lambda x : ''.join(x.astype(str)),axis = 1)
        # self.all_release_risk_df['Pattern'] = self.all_release_risk_df.iloc[:,1:len(self.all_release_risk_df.columns)-2].apply(lambda x: ''.join(str(x)), axis=1)
        print(self.all_release_risk_df)

        return self.all_release_risk_df

    def calAllRisk(self, release_info):
        self.ip_ed_df['Current_Date'] = release_info['HFS_Release_Date']
        risk_col_name = 'RTO_' + str(release_info['ReleaseNum'])

        risk_df = self.risk_window(self.ip_ed_df, None,
                                   'Current_Date',
                                   risk_col_name,
                                   self.risk_col_values,
                                   pat_info_flag=False)
        risk_df.reset_index(inplace=True)

        self.all_release_risk_df.loc[risk_df.index,risk_col_name] = risk_df[risk_col_name].astype(int)
        # self.all_release_risk_df[risk_col_name] = risk_df[risk_col_name].astype(int)

    def pat_info_risk_cal(self):

        self.risk_col_values = ['Low', 'Medium', 'High']

        self.enroll_df['Current_Date'] = self.max_date

        enrollment_risk_df = self.risk_window(self.enroll_df, self.ip_ed_df,
                                              'Initial_Enrollment_Date',
                                              'Enrollment_Risk',
                                              self.risk_col_values)

        current_risk_df = self.risk_window(self.enroll_df, self.ip_ed_df,
                                           'Current_Date', 'Current_Risk',
                                           self.risk_col_values)

        engagement_risk_df = self.risk_window(self.engage_df, self.ip_ed_df,
                                         'Engagement_Date', 'Engagement_Risk',
                                         self.risk_col_values)

        risk_cols = ['Enrollment_Risk', 'Engagement_Risk', 'Current_Risk']

        total_risk_df = pd.DataFrame(index=enrollment_risk_df.index,
                                     columns=risk_cols)

        total_risk_df.loc[engagement_risk_df.index,
                          'Engagement_Risk'] = engagement_risk_df['Engagement_Risk']
        total_risk_df.loc[current_risk_df.index,
                          'Current_Risk'] = current_risk_df['Current_Risk']
        total_risk_df.loc[enrollment_risk_df.index,
                          'Enrollment_Risk'] = enrollment_risk_df['Enrollment_Risk']
        total_risk_df.reset_index(inplace=True)

        return total_risk_df
