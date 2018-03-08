
import math
import numpy as np
import pandas as pd
from CHECK.dbconnect import dbconnect

connector = dbconnect.DatabaseConnect('CHECK_CPAR2')
# Date when first bills came in from HFS
check_start_date = '2014-05-01'
# Pulls patients in most recent ReleaseNum
pt_df = connector.query("""SELECT RecipientID,Program_Date,
                           pic.ReleaseNum,
                           '{}' as CHECK_Start_Date,
                           ri.HFS_Release_Date
                           from pat_info_complete pic
                           LEFT JOIN hfs_release_info ri on pic.ReleaseNum = ri.ReleaseNum""".format(check_start_date),
                        parse_dates=['Program_Date','HFS_Release_Date','CHECK_Start_Date'])

# goes back 6 months in time to be more bills were paid
pt_df['CHECK_End_Date'] = pt_df['HFS_Release_Date'].apply(lambda x: x - pd.DateOffset(months=6))
pt_df['Positive_Duration'] = ((pt_df['CHECK_End_Date'] - pt_df['Program_Date']))/np.timedelta64(1, 'M')
pt_df['Negative_Duration'] = ((pt_df['Program_Date'] - pt_df['CHECK_Start_Date']))/np.timedelta64(1, 'M')
pt_df['Positive_Duration'] = pt_df['Positive_Duration'].apply(math.floor)
pt_df['Negative_Duration'] = pt_df['Negative_Duration'].apply(math.floor)

cols = ['RecipientID','Program_Date','Negative_Duration','Positive_Duration','ReleaseNum']
connector.insert(pt_df[cols],'rid_pre_post_pat_windows')
