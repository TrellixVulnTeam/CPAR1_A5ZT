
import pandas as pd
import sys
import configparser
import itertools
import numpy as np
from dbconnect import dbconnect

connector = dbconnect.DatabaseConnect('CHECK_CPAR2')

relnum = sys.argv[0]

categories = {'IP':["m.recordidcd ='I'","m.Category1 ='INPATIENT'","m.category3 in ('INPATIENT_IP','EMERGENCY_IP')"],
              'EDIP':["m.recordidcd ='I'","m.Category1 ='INPATIENT'","m.category3 = 'EMERGENCY_IP'"],
              'EDOP':["m.recordidcd ='O'","m.Category1 ='OUTPATIENT'","m.category3 = 'EMERGENCY_OP'"]}

mc_df = connector.query("""select RecipientID, Category3 as 'OPCategory', ProviderNPI as 'OPProviderNPI',
                           CatgofServiceCd as 'OPCatgofServiceCd', AdjustedPriceAmt as 'OPAdjustedPriceAmt',
                           ServiceFromDt as 'EarliestServiceFromDt' from tsc_hfs_main_claims_new where Category3 = 'OUTPATIENT_OP' OR
                            Category3 = 'OTHER_OUTPATIENT_VISIT' OR
                            Category3 = 'RURAL_HEALTH_CLINIC_VISIT' OR
                            Category3 = 'HOSPITAL_OUTPATIENT_VISIT' OR
                            Category3 = 'PREVENTIVE_VISIT' OR
                            Category3 = 'OFFICE_VISIT' OR
                            Category3 = 'TELEPHONE_VISIT' OR
                            Category3 = 'HOME_VISIT' OR
                            Category3 = 'OUTPATIENT_CONSULT';""")

def final_report(cat_df, mc_df, category, relnum):

    op_ip_fil_df = pd.merge(cat_df, mc_df, on='RecipientID', how='left')
    op_ip_fil_df = op_ip_fil_df.loc[op_ip_fil_df['EarliestServiceFromDt']>op_ip_fil_df['DischargeDt']]
    gb = op_ip_fil_df.groupby(["RecipientID"],as_index=False).agg({"EarliestServiceFromDt":np.min})

    op_ip_fil_df = pd.merge(op_ip_fil_df,gb, on=["RecipientID","EarliestServiceFromDt"], how="inner")
    op_ip_fil_df = op_ip_fil_df.groupby(["RecipientID","EarliestServiceFromDt"],as_index=False).first()
    op_ip_fil_df.loc[(op_ip_fil_df['Engagement_Date']>op_ip_fil_df['DischargeDt']),'EngagementStatus'] = 'PreEngagement'
    op_ip_fil_df.loc[(op_ip_fil_df['Engagement_Date']<op_ip_fil_df['DischargeDt']),'EngagementStatus'] = 'PostEngagement'

    op_ip_fil_df['DaysDiff'] = op_ip_fil_df['EarliestServiceFromDt'] - op_ip_fil_df['DischargeDt']
    op_ip_fil_df['DaysDiff'] = op_ip_fil_df['DaysDiff']/np.timedelta64(1, 'D')

    code_desc_df = connector.query('''select CodeValue as CatgofServiceCd, CodeDescription as CodeDescription
                                      from hfs_cccd_code_descriptions where DomainName= 'CatgofServiceCd';''')

    op_ip_fil_df = pd.merge(op_ip_fil_df,code_desc_df, on='CatgofServiceCd', how="inner")
    code_desc_df.rename(columns={'CatgofServiceCd':'OPCatgofServiceCd',
                                 'CodeDescription':'OPCodeDescription'},inplace=True)

    op_ip_fil_df = pd.merge(op_ip_fil_df,code_desc1_df, on='OPCatgofServiceCd', how="inner")
    #there is no way this work
    op_ip_fil_df = op_ip_fil_df.drop(['recordIdCd','Category3'.format(category)],axis=1)

    op_ip_fil_df['Category'] = category
    op_ip_fil_df['Release_Number'] = relnum

    op_ip_fil_df = op_ip_fil_df[['Category','EngagementStatus','RecipientID','MRN','PatientID','Engagement_Date','AdmissionDt',
                                 'DischargeDt','ProviderNPI','CodeDescription','AdjustedPriceAmt','AdmissionDiagCd',
                                 'ICDVersion','OPCategory','OPProviderNPI','OPCodeDescription','OPAdjustedPriceAmt',
                                 'EarliestServiceFromDt','DaysDiff','Asthma','Diabetes','SCD','Prematurity','Epilepsy','Brain_Injury','Release_Number']]

    return op_ip_fil_df


for keys in categories.keys():
    if keys == 'EDOP':
        query_df = connector.query("""SELECT
        m.RecipientID,
        re.MRN,
        re.PatientID,
        m.ProviderNPI,
        m.ServiceFromDt AS 'AdmissionDt',
        m.ServiceThruDt AS 'DischargeDt',
        m.recordIdCd,
        m.Category3,
        m.CatgofServiceCd,
        m.AdjustedPriceAmt,
        re.Engagement_Date,
        NULL AS 'AdmissionDiagCd',
        NULL AS 'ICDVersion',
        re.Asthma,
        re.Diabetes,
        re.SCD,
        re.Prematurity,
        re.Epilepsy,
        re.Brain_Injury
    FROM
        tsc_hfs_main_claims_new m,
        pat_info_complete re
    WHERE
        m.RejectionStatusCd='N' AND
        {} AND
        {} AND
        {} AND
        re.RecipientID = m.RecipientID AND
        re.Engagement_Date is not null;""".format(categories[keys][1],categories[keys][2],categories[keys][0]))
    else:
        query_df = connector.query("""SELECT
        m.RecipientID,
        re.MRN,
        re.PatientID,
        m.ProviderNPI,
        i.AdmissionDt,
        i.DischargeDt,
        m.recordIdCd,
        m.Category3,
        m.CatgofServiceCd,
        m.AdjustedPriceAmt,
        re.Engagement_Date,
        i.AdmissionDiagCd,
        i.ICDVersion,
        re.Asthma,
        re.Diabetes,
        re.SCD,
        re.Prematurity,
        re.Epilepsy,
        re.Brain_Injury
    FROM tsc_hfs_institutional i,
        tsc_hfs_main_claims_new m,
        pat_info_complete re
    WHERE i.PatientStatusCd = '01'  AND
        i.RejectionStatusCd='N' AND
        i.DCN = m.DCN AND
        i.ServiceLineNbr = m.ServiceLineNbr AND
        i.RejectionStatusCd = m.RejectionStatusCd AND
        i.RecipientID = m.RecipientID AND
        i.AdjudicatedDt = m.AdjudicatedDt AND
        i.DischargeDt<>'0000-00-00'  AND
        {} AND
        {} AND
        {} AND
        re.RecipientID = m.RecipientID AND
        re.Engagement_Date is not null;""".format(categories[keys][1],categories[keys][2],categories[keys][0]))

    final_report_df = final_report(query_df, mc_df, keys, relnum)

    connector.insert(final_report_df,'rpt_hospitalization_category_op')
