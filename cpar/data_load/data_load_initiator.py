import fire
import sys
from CHECK.dbconnect import dbconnect
from CHECK.cpar.data_load.extract_and_rename_files import ExtractFiles
from CHECK.cpar.data_load.hfs_load_data import HFSLoadData
from CHECK.cpar.pat_info.dx_categorizer import DiagnosisMaster
from CHECK.cpar.data_load.raw_to_stage import raw_to_stage
from CHECK.cpar.cost_analysis.pt_window_load import pt_window_load
from CHECK.cpar.cost_analysis.pre_post_analysis import pre_post_analysis


class DataLoadInitiator(object):

    def __init__(self, release_num, release_date, database='CHECK_CPAR2',
                 data_source='HFS'):

        self.release_date = release_date
        self.database = database
        self.data_source = data_source
        self.conn = dbconnect.DatabaseConnect(self.database)
        self.release_num = release_num

    def initiate_process(self):

        print('ETL process initiated...')
        self.load_demo_data()
        self.load_raw_data()
        self.load_raw_to_stage_data()
        self.load_complete_pat_info()
        self.analysis()

    def load_demo_data(self):
        print('Loading demographics data...')
        self.conn.stored_procedure('pat_info_demo_load', self.release_num)
        print('Demographics data loaded')

    def load_raw_data(self):

        print('Initiated process to load raw tables')

        if ExtractFiles().create_and_unzip_files():
            HFSLoad = HFSLoadData(self.database, self.release_num,
                                  self.data_source)
            HFSLoad.load_data()
            HFSLoad.inline_loader()

    def load_raw_to_stage_data(self):

        print(raw_to_stage(self.release_date, self.release_num,
                     self.database).raw_stage())

    def load_complete_pat_info(self):

        print(DiagnosisMaster(self.database).load_diag_data())
        print(RiskCalculator(self.database, self.release_num).main())

    def analysis(self):

        self.conn.stored_procedure('rid_cost_insert')
        print(pt_window_load(self.database).window_load())
        pp_n_months = [6,12,18]
        for pp_n_month in pp_n_months:
            pre_post_analysis(pp_n_month, self.release_num,
                              seld.database).full_run(True)


if __name__ == '__main__':

     fire.Fire(DataLoadInitiator)
     # print('hello, lets start!')
     # DataLoadInitiator('234','34','CHECK_CPAR2', 'hfs').initiate_process()
