
import datetime

def fileNameDate(file_name,ext='.xlsx'):
    date = datetime.date.today()
    file_name = "{}_{}{}".format(file_name,date,ext)
    return file_name

def table_count_maker(df,groupby_list,count_value,presentation=False):
    '''Takes a pandas dataframe groups by list, counts rows and outputs a
    formatted dataframe for easy export. Presentation removes columns.
    To Do: It is currently built to only aggregate a list of length 2'''

    df_count = df.groupby(groupby_list,as_index=False).count()
    df_count = df_count.pivot(columns=groupby_list[0],index=groupby_list[1],values=count_value)
    #selects all values that will be counted
    vals = df[groupby_list[0]].unique()
    df_count['Total'] = df_count[vals].sum(axis=1)
    df_count['Total'] = df_count['Total'].astype(int)
    present_cols = []

    for i in vals:
        df_count[i].fillna(0,inplace=True)
        df_count[i] = df_count[i].astype(int)
        df_count[i+"_Perc"] = df_count[i]/df_count[i].sum(axis=0)
        df_count[i+"_Perc"] = df_count[i+"_Perc"].apply(lambda x: "{:.1%}".format(x))
        count_amount = int(df_count[i].sum())
        title = "{} (n={:d}) (%)".format(i, count_amount)
        present_cols.append(title)
        df_count[title] = df_count[i].astype(str) + ' ('+ df_count[i+'_Perc'] +")"

    df_count['Total_Perc'] = df_count['Total'] / df_count['Total'].sum()
    df_count['Total_Perc'] = df_count["Total_Perc"].apply(lambda x: "{:.1%}".format(x))
    df_count["Total (%)"] = df_count['Total'].astype(str) + ' ('+ df_count['Total_Perc'] +")"
    present_cols.append("Total (%)")
    if presentation == True:
        return df_count[present_cols]
    return df_count

def table_description_maker(df,groupby_list,desc_value,presentation=False):
    '''Returns a df that describes the average and std of a given group'''
    vals = list(df[groupby_list[0]].unique())
    vals.append('Total')
    df_desc = df.groupby(groupby_list).describe()
    df_desc = df_desc[[desc_value]].T

    total_pop = df.describe()[[desc_value]].T
    for i in total_pop:
        df_desc[('Total',i)] = total_pop[i]

    for col in df_desc.columns:
        df_desc[col] = df_desc[col].apply(lambda x: "{:.2f}".format(x))

    for i in vals:
        df_desc[('Final',i)] = (str(df_desc[(i,'mean')][0])+' ('+
                                        str(df_desc[(i,'std')][0])+')')
    if presentation == True:
        return df_desc['Final']
    return df_desc
