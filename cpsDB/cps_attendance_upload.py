import pandas as pd
import sys

#takes the suffix of a file and uploads the file to mysql

try:
    file_name = sys.argv[1]
    file_suffix = sys.argv[2]
except IndexError:
    print("Requires the attendance file path and the identifier YYYY-MM")

if len(suffix) != 7:
    raise InputError

pt_attendance_df = pd.read_csv(file_name,encoding = 'latin1')
pt_attendance_df = pt_attendance_df.rename(columns={'Medicaid RIN':'RIN'})
pt_attendance_df['RIN'] = pt_attendance_df['RIN'].astype(str).apply(PhoneMapHelper.medicaidNormalizer)
pt_attendance_df['DATE'] = pt_attendance_df['WEEK_DESC'].apply(CPSHelper.cpsDateCleaner)
col_names = CPSHelper.columnClean(pt_attendance_df)
pt_attendance_df.rename(columns=col_names,inplace=True)
pt_attendance_df['Student_Annual_Grade_Code'] = pt_attendance_df['Student_Annual_Grade_Code'].astype(str)
pt_attendance_df['Student_Birthdate'] = pd.to_datetime(pt_attendance_df['Student_Birthdate'])

pt_attendance_df['File_ID'] = file_suffix
pt_attendance_df['cdate'] = datetime.datetime.today()
pt_attendance_df['cdate'] = pt_attendance_df['cdate'].dt.date

toSQL.toSQL(pt_attendance_df,exist_method='append',table='cps_attendance')
