
import datetime
def fileNameDate(file_name,ext='.xlsx'):
    date = datetime.date.today()
    file_name = "{}_{}{}".format(file_name,date,ext)
    return file_name
