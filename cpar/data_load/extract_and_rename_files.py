import os
import zipfile
import datetime

current_dir = os.getcwd()
zip_files = [i for i in os.listdir(current_dir) if i.endswith('CCCDMonthlyUICheck.zip')]
assert len(zip_files) == 1


file_name = zip_files[0]
source_file_path = current_dir+"/source_data/"
output_file_path = current_dir+"/output_data/"
path_to_zip_file = current_dir+'/'+file_name
sql_file_path = current_dir+"/sql_scripts/"
try:
    if not os.path.exists(source_file_path):
        os.makedirs(source_file_path)
    if not os.path.exists(output_file_path):
        os.makedirs(output_file_path)
    if not os.path.exists(sql_file_path):
        os.makedirs(sql_file_path)
    if os.path.exists(path_to_zip_file):
        os.rename(path_to_zip_file, source_file_path+file_name)
except:
    print("Something went wrong")

#unzip the file from source folder to output folder

file_unzipped = zipfile.ZipFile(source_file_path+file_name, 'r')
file_unzipped.setpassword(b'SprinG1023!')
file_unzipped.extractall(output_file_path)
file_unzipped.close()
