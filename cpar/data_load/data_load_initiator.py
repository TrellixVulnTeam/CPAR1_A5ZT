import fire
import sys
from CHECK.dbconnect import dbconnect
from CHECK.cpar.data_load.extract_and_rename_files import ExtractFiles
from CHECK.cpar.data_load.hfs_load_data import HFSLoadData


class DataLoadInitiator(object):

    def __init__(self, release_num, release_date, database='CHECK_CPAR2',data_source='HFS'):

        self.release_date = release_date
        self.database = database
        self.data_source = data_source
        self.conn = dbconnect.DatabaseConnect(self.database)
        self.release_num = release_num

    def initiate_process(self):
        print('Initiated')
        self.conn.query("""Call pat_info_demo_load({})""".format(self.release_num), df_flag=False)
        if ExtractFiles().create_and_unzip_files():
            HFSLoad = HFSLoadData(self.database, self.release_num, self.data_source)
            HFSLoad.load_data()
            HFSLoad.inline_loader()


if __name__ == '__main__':
    #fire.Fire(DataLoadInitiator)
     print('hello, lets start!')
     DataLoadInitiator('234','34','CHECK_CPAR2', 'hfs').initiate_process()
