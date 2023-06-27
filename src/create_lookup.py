import pandas as pd
import dask.dataframe as dd

file_path = '/media/pedro/HD/Datasets/'

def get_lookup_df(month_name, year):
    df_ = dd.read_csv(file_path + 'light-'+month_name+year+'_0.csv', usecols = ['from_address', 'to_address']) #mudar isso para flashbots
    df__ = dd.read_csv(file_path + 'light-'+month_name+year+'_1.csv', usecols = ['from_address', 'to_address']) #mudar isso para flashbots
    if ((month_name == 'december' and year == '21') or (month_name == 'january' and year == '22')):
        df___ = dd.read_csv(file_path + 'light-'+month_name+year+'_2.csv', usecols = ['from_address', 'to_address']) #mudar isso para flashbots
        df_ = dd.concat([df_, df__, df___])
    else:
        df_ = dd.concat([df_,df__])
    del df__ 
    df_from = df_['from_address'].value_counts()
    df_to = df_['to_address'].value_counts()
    series_final = df_from.add(df_to, fill_value = 0)
    del df_from, df_to
    df_final = pd.DataFrame({month_name+year+'degree' : series_final.compute()})
    return df_final


option = input('Choose which months to create (1 to February - April; 2 to March - July; 3 to August - September; 4 to November - January)')
if option == '1':
    month_list = ['february', 'march', 'april']
    output_name = 'lookup_1.csv'
elif option == '2':
    month_list = ['may', 'june', 'july']
    output_name = 'lookup_2.csv'
elif option == '3':
    month_list = ['august', 'september', 'october']
    output_name = 'lookup_3.csv'
elif option == '4':
    month_list = ['november', 'december', 'january']
    output_name = 'lookup_4.csv'
lookup_df = pd.DataFrame({})
for month in month_list:
    year = '21'
    if month=='january':
        year = '22'
    print('Collecting '+month+'...')
    df = get_lookup_df(month, '21')
    df = pd.concat([df, lookup_df], axis = 1).fillna(0)
    print(month+' Collected successfully!')
    df.to_csv('./'+output_name)
    print('Lookup table was updated.')
    del df
    lookup_df = pd.read_csv('./'+output_name, index_col = 0)