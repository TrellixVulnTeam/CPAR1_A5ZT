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

    def main(self):
        tier_risk_df = RiskCategorizer().main(self.release_num, self.release_date)
        raw_risk_df = RiskScore().calculate_ucsd_risk()
        # merge the risk data
        total_risk_df = pd.merge(tier_risk_df, raw_risk_df, on='RecipientID', how='left')
        # insert data into db
        connector = dbconnect.DatabaseConnect(self.database)
        connector.replace(total_risk_df, 'pat_info_risk')

        return 'Risk calculation completed'
