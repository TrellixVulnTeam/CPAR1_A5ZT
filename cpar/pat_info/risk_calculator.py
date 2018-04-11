import pandas as pd
import sys
from CHECK.cpar.pat_info.ucsd_risk_score import RiskScore
from CHECK.cpar.pat_info.risk_tier_categorizer import RiskCategorizer
from CHECK.dbconnect import dbconnect


class RiskCalculator(object):

    def __init__(self, database, release_num, release_date):
        self.database = database
        self.release_num = release_num
        self.release_date = release_date
        self.connector = dbconnect.DatabaseConnect(self.database)
        self.risk_cat = RiskCategorizer()
        self.risk_cat.main(self.release_num, self.release_date)

    def cal_current_risk(self):

        tier_risk_df = self.risk_cat.pat_info_risk_cal()
        raw_risk_df = RiskScore().calculate_ucsd_risk()
        # merge the risk data
        total_risk_df = pd.merge(tier_risk_df, raw_risk_df, on='RecipientID',
                                 how='left')

        # insert data into db
        self.connector.replace(total_risk_df, 'pat_info_risk')

        return 'Risk calculation completed'

    def cal_all_release_risk(self):
        all_release_risk_df = self.risk_cat.all_release_risk_cal()

        # insert into databse by dropping the old table and creating a new one
        self.connector.query('drop table tuc_hfs_risk_all_release', False)
        self.connector.insert(all_release_risk_df, 'tuc_hfs_risk_all_release')

        # modify the column types in schema - 'tuc_hfs_risk_all_release'
        # in the database
        column_names = all_release_risk_df.columns[1:len(all_release_risk_df.columns)-2]
        for columns in column_names:
            self.connector.query('''ALTER TABLE tuc_hfs_risk_all_release
            CHANGE COLUMN {} {} TINYINT NULL DEFAULT NULL;'''.format(columns,columns),False)

        # for last table column
        self.connector.query('''ALTER TABLE tuc_hfs_risk_all_release
        CHANGE COLUMN {} {} Mediumint NULL DEFAULT NULL;'''.format('Total','Total'),False)

        return 'Risk Calculation for all releases completed'
