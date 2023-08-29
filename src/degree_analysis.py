import pandas as pd

class ETHDegreeAnalyzer:
    '''
    Class responsible for handling degree comparasions in Ethereum for different time periods.

    ----------

    #### Attributes:
    
    - df: DataFrame
        
        DataFrame object to be analyzed.

    ----------

    #### Methods:

    - get_descending_accounts(max_std, min_total_val, min_degree = -1)

            Creates and returns a DataFrame object containing all the accounts that had a sum of relative degree **higher** than a given threshold in 2021.
            Those accounts are expected to have a high number of transactions in 2021 compared to the war period.

    - get_ascending_accounts(max_std, max_total_val, min_degree = -1)

            Creates and returns a DataFrame object containing all the accounts that had a sum of relative degree **lower** than a given threshold in 2021.
            Those accounts are expected to have a low number of transactions in 2021 compared to the the war period.
    '''
    def __init__(self, df: pd.DataFrame):
        self._df = df
        self._2021_col_identifier = 'avgValue'

    def get_descending_accounts(self, max_std: float, min_total_val: float, min_degree: float = -1) -> pd.DataFrame:
        '''
        Constructs a dataframe with the accounts that had a high-ish degree in 2021 and decreased in the period of interest.

        ---

        #### Parameters:

        - max_std: float

            The maximum standard deviation allowed for the 2021 period. The lowest the value, the more stable the accounts will
            need to be to be selected.
        - min_total_val: float

            The minimum value for the summation of the relative degrees in 2021. The lowest the value, the lower the differences
            between 2021 and the war are going to be.
        - min_degree (optional): float

            The lowest degree for the resulting dataframe. Can be used to filter out little activity accounts.

        #### Returns: 
        
        DataFrame
        '''

        result = (
            self._df.query('stdVal > 0 and stdVal <= '+str(max_std)) 
            .query('totalVal > '+str(min_total_val)) 
            .sort_values(by = 'degree', ascending = False) 
        )
        return result if min_degree == -1 else result.query('degree >= '+str(min_degree))
    
    def get_ascending_accounts(self, max_std: float, max_total_val: float, min_degree: float = -1) -> pd.DataFrame:
        '''
        Constructs a dataframe with the accounts that had little activity (but existed!) in 2021 and increased a lot
        in the period of interest.

        ---

        #### Parameters:

        - max_std: float

            The maximum standard deviation allowed for the 2021 period. The lowest the value, the more stable the accounts will
            need to be to be selected.
        - max_total_val: float

            The maximum value for the summation of the relative degrees in 2021. The lowest the value, the bigger the differences
            between 2021 and the war are going to be.
        - min_degree (optional): float

            The lowest degree for the resulting dataframe. Can be used to filter out little activity accounts.

        #### Returns: 
        
        DataFrame
        '''


        columns_2021 = [col for col in self._df.columns if col.startswith(self._2021_col_identifier)]
        exists_in_2021 = ' and '.join(f'{column} > 0' for column in columns_2021)
        result = (
            self._df.query(exists_in_2021)
            .query('stdVal > 0 and stdVal < '+str(max_std))
            .query('totalVal <= '+str(max_total_val))
            .sort_values(by = 'degree', ascending = False)
        )   
        return result if min_degree == -1 else result.query('degree >= '+str(min_degree))