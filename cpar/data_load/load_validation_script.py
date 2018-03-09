import pandas as pd
import numpy as np
from dbconnect import dbconnect

connector = dbconnect.DatabaseConnect('CHECK_CPAR2')

load_valid_dict = {'trc_hfs_adjustments':'adjustedclaimextractuick1.out',
             'trc_hfs_main_claims':'claim_finaluick1.out',
             'trc_hfs_nips':'servicenips_finaluick1.out',
             'trc_hfs_pharmacy':'pharmacy_finaluick1.out',
             'trc_hfs_procedure':'serviceproc_finaluick1.out',
             'trc_hfs_recipient_flags':'recipientflags_final_uick1.out',
             'trc_hfs_revenue_codes':'servicerev_finaluick1.out',
             'trc_hfs_compound_drugs_detail':'servicepharmndc_finaluick1.out',
             'trc_hfs_cornerstone_immunization':'cornerstone_finaluick1.out',
             'trc_hfs_diagnosis':'servicediag_finaluick1.out',
             'trc_hfs_institutional':'serviceinst_finaluick1.out',
             'trc_hfs_lead':'lead_finaluick1.out'}

tables_count_dict = {}
files_count_dict = {}
not_match = []

rel_num = connector.query('''Select max(ReleaseNum) from pat_info_demo;''')
rel_num = rel_num['max(ReleaseNum)']
rel_num[0]

for tables in load_valid_dict:
    tbl_count = connector.query('''Select count(*) as count from {} where ReleaseNum = {}'''.format(tables, rel_num[0]))
    tbl_count = tbl_count['count']
    tables_count_dict[tables] = tbl_count[0]
    #Replace the path of the file location in the production
    with open('C://Work//source_files//{}'.format(load_valid_dict[tables])) as f:
        files_count_dict[tables] = sum(1 for _ in f)
    
    if(tables_count_dict[tables] != files_count_dict[tables]):
        not_match.append(tables)

if len(not_match) == 0:
    print("The row count in tables and files match")
else:
    print("The row count in following tables do not match with the files")
    print(not_match)
    for tables in load_valid_dict:
        connector.query('''Delete from {} where ReleaseNum = {}'''.format(tables, rel_num[0]))
