import pandas as pd
from CHECK.dbconnect import dbconnect

class raw_to_stage():
    def __init__(self, release_date, release_num, db):

        self.start_date = '2013-01-01'
        self.db = db
        self.connector = dbconnect.DatabaseConnect(self.db)
        self.release_date = release_date
        self.release_num = release_num


    def op_categorization(self):
        df_cat = self.connector.query('''select a.RecipientID,a.RejectionStatusCd, a.ServiceLineNbr, a.AdjudicatedDt, a.DCN,a.RevenueCd,
        a.RevenueHCPCSCd, b.Category1, b.Category2, b.Category3, b.Category2Rank, b.Category3Rank from tsc_hfs_revenue_codes a
        left join hfs_categorization_tbl b on a.RevenueCd = b.RevenueCd and a.RevenueHCPCSCd = b.RevenueHCPCSCd;''')

        df_cat['Category1'] = df_cat['Category1'].fillna('OUTPATIENT')
        df_cat['Category2'] = df_cat['Category2'].fillna('UNCLASSIFIED')
        df_cat['Category3'] = df_cat['Category3'].fillna('UNCLASSIFIED')
        df_cat['Category2Rank'] = df_cat['Category2Rank'].fillna(15)
        df_cat['Category3Rank'] = df_cat['Category3Rank'].fillna(15)

        OP_list = self.connector.query('''select distinct RevenueHCPCSCd from hfs_categorization_tbl where Category1 = 'OUTPATIENT' and Category3 = 'OUTPATIENT_OP';''')
        df_cat.loc[df_cat['RevenueHCPCSCd'].isin(OP_list['RevenueHCPCSCd']),
                  ['Category2','Category3','Category2Rank','Category3Rank']] = ['OUTPATIENT', 'OUTPATIENT_OP', 3, 3]

        OP_AMB_list = self.connector.query('''Select distinct RevenueHCPCSCd from hfs_categorization_tbl where Category1= 'OUTPATIENT' and Category3 = 'Ambulance_OP';''')
        df_cat.loc[df_cat['RevenueHCPCSCd'].isin(OP_AMB_list['RevenueHCPCSCd']),
                  ['Category2','Category3','Category2Rank','Category3Rank']] = ['ANCILLARY',  'AMBULANCE_OP', 8, 8]

        OP_DME_list = self.connector.query('''Select distinct RevenueHCPCSCd from hfs_categorization_tbl where Category1= 'OUTPATIENT' and Category3 = 'DME_OP';''')
        df_cat.loc[df_cat['RevenueHCPCSCd'].isin(OP_DME_list['RevenueHCPCSCd']),
                  ['Category2','Category3','Category2Rank','Category3Rank']] = ['ANCILLARY', 'DME_OP', 9, 9]

        OP_HH_list = self.connector.query('''Select distinct RevenueHCPCSCd from hfs_categorization_tbl where Category1= 'OUTPATIENT' and Category3 = 'HOME_HEALTH_OP';''')
        df_cat.loc[df_cat['RevenueHCPCSCd'].isin(OP_HH_list['RevenueHCPCSCd']),
                  ['Category2','Category3','Category2Rank','Category3Rank']] = ['ANCILLARY', 'HOME_HEALTH_OP', 10, 10]

        df_cat = df_cat.loc[df_cat.groupby("DCN")["Category2Rank"].idxmin()]
        df_claims = self.connector.query('''Select a.RecipientID, a.RejectionStatusCd, a.ServiceLineNbr, a.AdjudicatedDt, a.DCN, a.RecordIDCD,
        a.ServiceFromDt, a.ServiceThruDt from tsc_hfs_main_claims a where RejectionStatusCd ='N' and RecordIDCD = 'O'; ''')
        df_final = df_cat.merge(df_claims, on=('RecipientID','RejectionStatusCd','ServiceLineNbr','AdjudicatedDt','DCN'), how='inner')
        df_final = df_final[['RecipientID','RejectionStatusCd','ServiceLineNbr','AdjudicatedDt','DCN','RecordIDCD',
        'ServiceFromDt','ServiceThruDt','Category1','Category2','Category3','Category2Rank','Category3Rank']]

        self.connector.replace(df_final,'hfs_categorize_outpatient')

    def raw_stage(self):
        self.connector.stored_procedure('proc_wrapper_move_hfs_raw_to_stage',
                                        [self.start_date,self.release_date,self.release_num])
        self.op_categorization()
        self.connector.stored_procedure('proc_get_sc_hfs_main_claims_new',
                                        [self.start_date,self.release_date,self.release_num])
        self.connector.stored_procedure('proc_update_main_claims_new')
        return "Raw to stage complete"
