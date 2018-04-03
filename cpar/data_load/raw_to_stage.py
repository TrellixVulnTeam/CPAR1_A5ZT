import pandas as pd
import os
from CHECK.dbconnect import dbconnect
from CHECK.cpar.data_load import mcn_categorization

class raw_to_stage():
    def __init__(self, release_date, release_num, db):

        self.start_date = '2013-01-01'
        self.db = db
        self.connector = dbconnect.DatabaseConnect(self.db)
        self.release_date = release_date
        self.release_num = release_num


    def mcn_cat_upload(self, mcn_file_name='mcn_categorization.csv'):
        '''runs mcn categorization then uploads to database'''

        cat_obj = mcn_categorization.mcn_categorization(db_name=self.db,
                                                        release_num=self.release_num)
        mcn_cat = cat_obj.full_run()
        mcn_cat.to_csv(mcn_file_name, chunksize=10000, index=False)
        self.connector.query("TRUNCATE tsc_hfs_main_claims_new_categorization;", df_flag=False)
        load_file = """LOAD DATA LOCAL INFILE '{}' INTO TABLE tsc_hfs_main_claims_new_categorization
        FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES""".format(mcn_file_name)
        self.connector.inline_import(load_file,mcn_file_name)


    def raw_stage(self):
        self.connector.stored_procedure('proc_wrapper_move_hfs_raw_to_stage',
                                        [self.start_date,self.release_date,self.release_num])
        self.connector.stored_procedure('proc_get_sc_hfs_main_claims_new',
                                        [self.start_date,self.release_date,self.release_num])
        mcn_file_name = 'mcn_categorization.csv'
        self.mcn_cat_upload(mcn_file_name)
        self.connector.stored_procedure('update_mcn')
        os.remove(mcn_file_name)
        return "Raw to stage complete"
