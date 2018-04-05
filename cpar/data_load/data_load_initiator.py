#!/usr/bin/env python
import fire
import sys
import pandas as pd
import datetime
from CHECK.dbconnect import dbconnect
from CHECK.cpar.data_load.extract_and_rename_files import ExtractFiles
from CHECK.cpar.data_load.hfs_load_data import HFSLoadData
from CHECK.cpar.pat_info.dx_categorizer import DiagnosisMaster
from CHECK.cpar.pat_info.risk_calculator import RiskCalculator
from CHECK.cpar.data_load.raw_to_stage import raw_to_stage
from CHECK.cpar.cost_analysis.pt_window_load import pt_window_load
from CHECK.cpar.cost_analysis.pre_post_analysis import pre_post_analysis
from CHECK.slack.slack_log import cli_output


class DataLoadInitiator(object):

    def __init__(self, release_num, release_date, database='CHECK_CPAR2',
                 data_source='HFS'):

        self.release_date = release_date
        self.database = database
        self.data_source = data_source
        self.conn = dbconnect.DatabaseConnect(self.database)
        self.release_num = release_num

    def initiate_process(self):
        etl_start_string = 'ETL process initiated for ReleaseNum: {}'.format(self.release_num)
        cli_output(etl_start_string)
        self.load_release_info()
        self.load_demo_data()
        self.load_raw_data()
        self.load_raw_to_stage_data()
        self.load_complete_pat_info()
        self.analysis()
        cli_output("Data Load Complete!")

    def load_release_info(self):

        if(self.conn.query('''select max(ReleaseNum) as ReleaseNum from
                              hfs_release_info''')['ReleaseNum'][0] != self.release_num):
            release_info_df = pd.DataFrame([[self.release_num,
                                             str(self.release_num)[-2:],
                                             self.release_date,
                                             datetime.datetime.now().strftime("%Y-%m-%d")]],
                                           columns=['ReleaseNum',
                                                    'Cumulative_ReleaseNum',
                                                    'HFS_Release_Date',
                                                    'Load_Date'])
            self.conn.insert(release_info_df,'hfs_release_info')

    def load_demo_data(self):

        cli_output('Loading demographics data...')
        self.conn.stored_procedure('pat_info_demo_load', [self.release_num,self.release_date])
        cli_output('Demographics data loaded')

    def load_raw_data(self):

        cli_output('Initiated process to load raw tables')

        if ExtractFiles().create_and_unzip_files():
            HFSLoad = HFSLoadData(self.database, self.release_num,
                                  self.data_source)
            HFSLoad.load_data()
            HFSLoad.inline_loader()
        cli_output('load_raw_data complete')


    def load_raw_to_stage_data(self):

        cli_output('Loading data from raw to staging tables...')
        cli_output(raw_to_stage(self.release_date, self.release_num, self.database).raw_stage())

    def load_complete_pat_info(self):

        cli_output('Processing and loading diagnosis categorization data...')
        cli_output(DiagnosisMaster(self.database).load_diag_data())
        cli_output('Calculating and loading risk data...')
        cli_output(RiskCalculator(self.database, self.release_num,
                             self.release_date).main())
        cli_output('Loading data into pat_info_complete table...')
        self.conn.stored_procedure('pat_info_demo_complete_generation',
                                   [self.release_num, self.release_date])
        cli_output('Data load for pat_info_complete done.')

    def analysis(self):

        cli_output('Cost analysis started...')
        self.conn.stored_procedure('rid_cost_insert')
        cli_output('Loading data to pt_window')
        cli_output(pt_window_load(self.release_num, self.release_date,
                             self.database).window_load())
        pp_n_months = [6,12,18]
        for pp_n_month in pp_n_months:
            cli_output('''Pre and post analysis for %s months period
                     started...''' %(pp_n_month))
            pre_post_analysis(pp_n_month, self.release_num,
                              self.database).full_run(True)
            cli_output('Analysis completed.')


if __name__ == '__main__':

     fire.Fire()
     # print('hello, lets start!')
     # DataLoadInitiator('234','34','CHECK_CPAR2', 'hfs').initiate_process()
