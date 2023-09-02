import pandas as pd
import dask.dataframe as dd

file_path = '/media/pedro/HD/Datasets/'

def get_lookup_df(month_name, year, type, is_war):
    if is_war:
        df_ = dd.read_csv(file_path + 'fix_war_txs.csv', 
                        usecols = ['from', 'to'])
        df__ = dd.read_csv(file_path + 'fix_war_txs_2.csv', 
                        usecols = ['from', 'to'])
        df___ = dd.read_csv(file_path + 'fix_war_txs_3.csv', 
                            usecols = ['from', 'to'])
        df____ = dd.read_csv(file_path + 'fix_war_txs_4.csv',
                             usecols = ['from', 'to'])
        df_ = dd.concat([df_, df__, df___, df____])
        del df__, df___, df____ 

        df_from = df_['from'].value_counts()
        df_to = df_['to'].value_counts()
        if type == '_in':
            series_final = df_to
            del df_from
        elif type == '_out':
            series_final = df_from
            del df_to
        else:
            series_final = df_from.add(df_to, fill_value = 0)
            del df_from, df_to
        df_final = pd.DataFrame({'degree' : series_final.compute()})
        return df_final


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
    if type == '_in':
        series_final = df_to
        del df_from
    elif type == '_out':
        series_final = df_from
        del df_to
    else:
        series_final = df_from.add(df_to, fill_value = 0)
        del df_from, df_to
    
    df_final = pd.DataFrame({month_name+year+'degree' : series_final.compute()})
    #df_final[month_name+year+'_value'] = [df_.loc[i]['value'] for i, _ in df_final.iterrows()]

    return df_final



option = input('Choose which months to create (1 to February - April; 2 to March - July; 3 to August - September; 4 to November - January; 5 to Russian Invasion)')
is_war = False 
degree_type = ''
if option == '1':
    month_list = ['february', 'march', 'april']
    output_name = 'lookup_1'
elif option == '2':
    month_list = ['may', 'june', 'july']
    output_name = 'lookup_2'
elif option == '3':
    month_list = ['august', 'september', 'october']
    output_name = 'lookup_3'
elif option == '4':
    month_list = ['november', 'december', 'january']
    output_name = 'lookup_4'
elif option == '5':
    month_list = ['war']
    output_name = 'lookup_war'
    is_war = True
lookup_df = pd.DataFrame({})
for month in month_list:
    year = '21'
    if month=='january':
        year = '22'
    print('Collecting '+month+'...')
    df = get_lookup_df(month, year, degree_type, is_war)
    df = pd.concat([df, lookup_df], axis = 1).fillna(0)
    print(month+' Collected successfully!')
    df.to_csv('./'+output_name+degree_type+'.csv')
    print('Lookup table was updated.')
    del df
    lookup_df = pd.read_csv('./'+output_name+degree_type+'.csv', index_col = 0)