import os
import zipfile
from CHECK.secret import secret


class ExtractFiles(object):

    def __init__(self):

        self.current_dir = os.getcwd()
        self.source_file_path = self.current_dir + "/source_data/"
        self.output_file_path = self.current_dir + "/output_data/"
        self.sql_file_path = self.current_dir + "/sql_scripts/"
        self._sec = secret.secret()

    def create_and_unzip_files(self):

        if os.path.exists(self.output_file_path) and len(
        [i for i in os.listdir(self.output_file_path)
         if not i.startswith('.')]) == 13:
            print('Files already present in output_data folder')
            return True
        else:
            print('deleting incomplete output_files')
            self.delete_files(self.output_file_path)

        if os.path.exists(self.source_file_path):
            source_files = [i for i in os.listdir(self.source_file_path)
                            if not i.startswith('.')]
            if len(source_files) == 1:
                print('Zip file present in source_data folder')
                self.file_name = source_files[0]
                if not os.path.exists(self.output_file_path):
                    os.makedirs(self.output_file_path)
                self.unzip_files()
                return True

        zip_files = [i for i in os.listdir(self.current_dir)
                     if i.endswith('CCCDMonthlyUICheck.zip')]

        if len(zip_files) != 1:
            print("Zip files not present in the current directory ",
                  self.current_dir)
            return False
        else:
            self.file_name = zip_files[0]
            self.path_to_zip_file = self.current_dir + '/' + self.file_name
            try:
                if not os.path.exists(self.source_file_path):
                    os.makedirs(self.source_file_path)
                if not os.path.exists(self.output_file_path):
                    os.makedirs(self.output_file_path)
                if not os.path.exists(self.sql_file_path):
                    os.makedirs(self.sql_file_path)
                if os.path.exists(self.path_to_zip_file):
                    os.rename(self.path_to_zip_file,
                              self.source_file_path+self.file_name)
                self.unzip_files()
            except:
                    print("Something went wrong")

        return True

    def unzip_files(self):
        print('extracting files...')

        # unzip the file from source folder to output folder
        file_unzipped = zipfile.ZipFile(self.source_file_path + self.file_name,
                                        'r')
        file_unzipped.setpassword(self._sec.getZip())
        file_unzipped.extractall(self.output_file_path)
        file_unzipped.close()

    def delete_files(self, folder):

        if os.path.exists(folder):
            files_to_delete = [i for i in os.listdir(folder)
                               if not i.startswith('.')]
            print('incomplete files to delete: ', len(files_to_delete))
            if len(files_to_delete) > 0:
                for i in files_to_delete:
                    try:
                        os.remove(i)
                    except:
                        pass
