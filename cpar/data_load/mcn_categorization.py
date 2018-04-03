import os
import pandas as pd
import numpy as np
from CHECK.dbconnect import dbconnect
from CHECK.conconnect import conconnect


class mcn_categorization():

    def __init__(self, release_num, db_name):
        self.release_num = release_num
        self.db_name = db_name
        self.connector = dbconnect.DatabaseConnect(self.db_name)
        self.primary_keys = ['DCN','ServiceLineNbr','RejectionStatusCd','RecipientID','AdjudicatedDt']
        self.output_cols = ['RecipientID','RejectionStatusCd','ServiceLineNbr','AdjudicatedDt',
                            'DCN','Category1','Category2','Category3','Category2Rank','Category3Rank',
                            'NetLiabilityAmt', 'EncounterPriceAmt','Visit','Service_Count','Procedure_Count',
                            'Encounter','Visit_Inpatient_Days']


    def code_table_intializer(self):
        query = conconnect.ConsensusConnect()
        self.mcn = query.mcn_categorization_query()
        self.rv_codes = query.revenue_code_query()
        self.ip_codes = query.ip_code_query()
        self.pc_codes = query.procedure_code_query()
        self.nip_codes = query.nip_code_query()
        self.rv_codes = query.revenue_code_query()
        self.bill_categorization = query.bill_categorizer_query()

    def ip_categorization(self, mcn, ip_codes, rv_codes, pc_codes):

        ip_bills = mcn.loc[mcn['RecordIDCd']=='I']
        ip_category = pd.merge(ip_bills, ip_codes, how='inner', left_index=True, right_index=True)

        ip_category['Category1'] = 'INPATIENT'
        ip_category['Category2'] = np.nan
        ip_category['Category3'] = np.nan

        cat2_ip = [111, 112, 113, 114, 115, 116, 117, 118, 119, 121,
                   122, 123, 124, 125, 126, 127, 128, 129]
        cat2_snf = [211,212,213,214,215,216,217,218,219,221,222,223,
                    224,225,226,227,228,229,231,232,233,234,235,236,
                    237,238,239,241,242,243,244,245,246,247,248,249,
                    251,252,253,254,255,256,257,258,259,261,262,263,
                    264,265,266,267,268,269,271,272,273,274,275,276,
                    277,278,279,281,282,283,284,285,286,287,288,289]
        cat2_hsp = [811,812,813,814,815,816,817,818,819,821,822,823,
                    824,825,826,827,828,829]

        ip_category.loc[ip_category['UBTypeofBillCd'].isin(cat2_ip),'Category2'] = 'INPATIENT'
        ip_category.loc[ip_category['UBTypeofBillCd'].isin(cat2_snf),'Category2'] = 'SNF'
        ip_category.loc[ip_category['UBTypeofBillCd'].isin(cat2_hsp),'Category2'] = 'HOSPICE'
        ip_category.loc[ip_category['Category2'].isnull(),'Category2'] = 'OTHER'

        ip_rv_codes = rv_codes.loc[ip_category.index]
        ip_rv_codes.index.names = self.primary_keys
        emergency_ip = ip_rv_codes.loc[ip_rv_codes['RevenueCd'].isin(('0450','0451','0452','0456','0459','0981'))].index
        ip_category.loc[emergency_ip,'Category3'] = 'EMERGENCY_IP'

        ip_category.loc[(ip_category['Category2']=='INPATIENT')&
                        (ip_category['Category3']!='EMERGENCY_IP'),'Category3'] = 'INPATIENT_IP'

        ip_category.loc[ip_category['Category3'].isnull(),'Category3'] =  ip_category.loc[ip_category['Category3'].isnull(),'Category2'] +'_IP'

        ip_category['Category2Rank'] = 1.5
        ip_category['Category3Rank'] = 1.5
        #Service Counts
        rev_service_codes = ip_rv_codes.loc[ip_rv_codes['RevenueCd']!='0001'].reset_index()
        ip_service_counts = rev_service_codes.groupby(self.primary_keys)[['RevenueCd']].count()
        ip_service_counts = ip_service_counts.rename(columns={'RevenueCd':'Service_Count'})
        ip_category = pd.merge(ip_category,ip_service_counts,how='left',left_index=True,right_index=True)
        #procedure count
        ip_pc = pd.merge(ip_category,pc_codes,how='left',left_index=True,right_index=True)[['ProcCd']].reset_index()
        ip_pc = ip_pc.groupby(self.primary_keys).count().rename(columns={'ProcCd':'Procedure_Count'})
        ip_category = pd.merge(ip_category,ip_pc,how='left',left_index=True,right_index=True)
        ip_category = ip_category.reset_index()
        # adds visits to largest line nbr
        ip_category['Visit'] = 0
        ip_category.loc[ip_category.groupby(['RecipientID','DCN',
                                             'RejectionStatusCd'])['ServiceLineNbr'].transform('idxmax'),'Visit'] = 1
        # add encounter to max ServiceLineNbr by dcn
        ip_category['Encounter'] = 0
        ip_category.loc[ip_category.groupby(['RecipientID','DCN',
                                             'RejectionStatusCd'])['ServiceLineNbr'].transform('idxmax'),'Encounter'] = 1
        # inpatient day counts
        inpatient_days = ip_category.groupby(['RecipientID','DCN']).agg({'ServiceFromDt':np.min,
                                                                         'ServiceThruDt':np.max,
                                                                         'ServiceLineNbr':np.max})
        inpatient_days['Visit_Inpatient_Days']= ((inpatient_days['ServiceThruDt'] -
                                                  inpatient_days['ServiceFromDt']).dt.days)+1
        inpatient_days = inpatient_days.drop(['ServiceThruDt','ServiceFromDt'],axis=1).reset_index()
        ip_category = pd.merge(ip_category, inpatient_days, how='left', on=['RecipientID','ServiceLineNbr','DCN'])
        ip_category['Visit_Inpatient_Days'] = ip_category['Visit_Inpatient_Days'].fillna(0)
        return ip_category[self.output_cols]


    def op_categorization(self,mcn,bill_categorization,rv_codes):

        op_categorization = bill_categorization.loc[bill_categorization['Category1']=='OUTPATIENT']
        op_bills = mcn.loc[mcn['RecordIDCd']=='O']
        op_bills = pd.merge(op_bills,rv_codes,left_index=True,right_index=True)

        op_merge = pd.merge(op_bills.reset_index(),op_categorization,on=['RevenueCd','RevenueHCPCSCd'],how='left')
        op_merge['Category1'] = op_merge['Category1'].fillna('OUTPATIENT')
        op_merge['Category2'] = op_merge['Category2'].fillna('UNCLASSIFIED')
        op_merge['Category3'] = op_merge['Category3'].fillna('UNCLASSIFIED')
        op_merge['Category2Rank'] = op_merge['Category2Rank'].fillna(15)
        op_merge['Category3Rank'] = op_merge['Category3Rank'].fillna(15)

        OP_list = op_categorization.loc[op_categorization['Category3']=='OUTPATIENT_OP','RevenueHCPCSCd'].unique()
        op_merge.loc[op_merge['RevenueHCPCSCd'].isin(OP_list),
                      ['Category2','Category3','Category2Rank','Category3Rank']] = ['OUTPATIENT', 'OUTPATIENT_OP', 3, 3]

        OP_AMB_list = op_categorization.loc[op_categorization['Category3']=='AMBULANCE_OP','RevenueHCPCSCd'].unique()

        op_merge.loc[op_merge['RevenueHCPCSCd'].isin(OP_AMB_list),
                    ['Category2','Category3','Category2Rank','Category3Rank']] = ['ANCILLARY',  'AMBULANCE_OP', 8, 8]

        OP_DME_list = op_categorization.loc[op_categorization['Category3']=='DME_OP','RevenueHCPCSCd'].unique()
        op_merge.loc[op_merge['RevenueHCPCSCd'].isin(OP_DME_list),
                     ['Category2','Category3','Category2Rank','Category3Rank']] = ['ANCILLARY', 'DME_OP', 9, 9]

        OP_HH_list = op_categorization.loc[op_categorization['Category3']=='HOME_HEALTH_OP','RevenueHCPCSCd'].unique()

        op_merge.loc[op_merge['RevenueHCPCSCd'].isin(OP_HH_list),
                       ['Category2','Category3','Category2Rank','Category3Rank']] = ['ANCILLARY', 'HOME_HEALTH_OP', 10, 10]
        op_merge = op_merge.sort_values(self.primary_keys+['Category2Rank','Category3Rank'])
        op_merge = op_merge.drop_duplicates(subset=self.primary_keys)
        op_merge['Visit'] = 1
        service_count = op_bills.loc[op_bills['RevenueCd']!='0001'].reset_index()
        service_count = service_count.groupby(['RecipientID','DCN'])[['RevenueCd']].count()
        service_count = service_count.rename(columns={'RevenueCd':'Service_Count'}).reset_index()
        op_merge = pd.merge(op_merge, service_count, how='left', on=['RecipientID','DCN'])
        op_merge['Service_Count'] = op_merge['Service_Count'].fillna(0)
        op_merge['Encounter'] = 1
        op_merge['Procedure_Count'] = 0
        op_merge['Visit_Inpatient_Days'] = 0
        return op_merge[self.output_cols]

    def nips_categorization(self,mcn,bill_categorization,nip_codes,pc_codes):

        nip_cats = bill_categorization.loc[bill_categorization['Category1']=='NIPS']
        nip_bills = mcn.loc[mcn['RecordIDCd']=='N']

        nip_bills = pd.merge(nip_bills, nip_codes, how='left', left_index=True, right_index=True)
        nip_bills = pd.merge(nip_bills, pc_codes, how='left', left_index=True, right_index=True)
        nip_merge = pd.merge(nip_bills.reset_index(),nip_cats,how='left',on=['ProcCd'])
        # where there is a match in PlaceOfServiceCd it becomes highest priority
        nip_merge.loc[nip_merge['PlaceOfServiceCd_x']==nip_merge['PlaceOfServiceCd_y'],'Category2Rank'] = 0
        nip_merge['Category1'] = nip_merge['Category1'].fillna('NIPS')
        nip_merge['Category2'] = nip_merge['Category2'].fillna('OTHER')
        nip_merge['Category3'] = nip_merge['Category3'].fillna('OTHER_PROF')
        nip_merge[['Category2Rank','Category3Rank']] = nip_merge[['Category2Rank','Category3Rank']].fillna(15.0)

        nip_merge = nip_merge.sort_values(self.primary_keys+['Category2Rank','Category3Rank'])
        nip_merge = nip_merge.drop_duplicates(subset=self.primary_keys)
        nip_merge['Visit'] = 0
        nip_merge.loc[nip_merge['Category3'].isin(('OTHER_OUTPATIENT_VISIT' ,
                                                   'HOSPITAL_OUTPATIENT_VISIT',
                                                   'OFFICE_VISIT','RURAL_HEALTH_CLINIC_VISIT',
                                                   'HOME_VISIT','TELEPHONE_VISIT',
                                                   'PREVENTIVE_VISIT','OUTPATIENT_CONSULT')),'Visit'] = 1
        nip_merge['Service_Count'] = 1
        nip_merge['Encounter'] = 1
        nip_merge['Procedure_Count'] = 0
        nip_merge['Visit_Inpatient_Days'] = 0
        return nip_merge[self.output_cols]

    def mcn_categorization(self, mcn, bill_categorization, rv_codes, nip_codes, pc_codes, ip_codes):
        op_bill_cat = self.op_categorization(mcn, bill_categorization, rv_codes)
        nip_bill_cat = self.nips_categorization(mcn, bill_categorization, nip_codes, pc_codes)
        ip_bill_cat = self.ip_categorization(mcn, ip_codes, rv_codes, pc_codes)

        check_cat = self.connector.query('''SELECT Category1, Category2, Category3, CHECK_Category FROM hfs_categorization_check''')
        check_cat['Category1'] = check_cat['Category1'].astype('category')
        check_cat['Category2'] = check_cat['Category2'].astype('category')
        check_cat['Category3'] = check_cat['Category3'].astype('category')

        mcn_cat = pd.concat([op_bill_cat, nip_bill_cat, ip_bill_cat])
        mcn_cat['AdjustedPriceAmt'] = mcn_cat[['NetLiabilityAmt','EncounterPriceAmt']].max(axis=1)
        mcn_cat = mcn_cat.drop(['NetLiabilityAmt','EncounterPriceAmt'],axis=1)
        mcn_cat['Category1'] = mcn_cat['Category1'].astype('category')
        mcn_cat['Category2'] = mcn_cat['Category2'].astype('category')
        mcn_cat['Category3'] = mcn_cat['Category3'].astype('category')

        mcn_cat = pd.merge(mcn_cat, check_cat,how='left',on=['Category1','Category2','Category3'])
        return mcn_cat


    def full_run(self):
        self.code_table_intializer()
        mcn_cat = self.mcn_categorization(self.mcn, self.bill_categorization, self.rv_codes,
                                          self.nip_codes, self.pc_codes, self.ip_codes)
        mcn_cat['Prematurity_Ineligible'] = 0
        mcn_cat = mcn_cat[['DCN','ServiceLineNbr','RejectionStatusCd','RecipientID','AdjudicatedDt','AdjustedPriceAmt',
        'Category1','Category2','Category3','CHECK_Category','Category2Rank','Category3Rank','Visit',
        'Service_Count','Procedure_Count','Encounter','Visit_Inpatient_Days','Prematurity_Ineligible']]

        mcn_cat = mcn_cat.sort_values(self.primary_keys)
        return mcn_cat
