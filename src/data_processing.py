from enum import Enum
import pandas as pd

class DegreeType(Enum):
    IN_DEGREE = 1
    OUT_DEGREE = 2
    DEGREE = 3
    
class ETHDataProcessor:
    '''
    Collects all the lookup datasets and preprocess all of them to be used in the ETHDegreeAnalyzer class.

    ----------

    #### Attributes:
    
    - datasets_name: str

            Name used for the filenames of the lookup datasets. Defaults to "lookup".

    - lookup_path: str
    
            Path for the lookup dataset files that will be used for the comparasions. Defaults to the current working directory.

    - degree_type: DegreeType

            Type of degree analysis to be performed. Acceps DegreeType.IN_DEGREE, DegreeType.OUT_DEGREE and DegreeType.DEGREE. 
            Defaults to DegreeType.DEGREE.

    ----------

    #### Methods:

    - get_df()

            Loads the dataset in the memory, prepares it and returns it to be used in the ETHDegreeAnalyzer class.
    '''
    def __init__(
                self,
                file_name: str = 'lookup', 
                file_path: str = './',
                degree_type: DegreeType = DegreeType.DEGREE
        ):
        self.file_name = file_name
        self.file_path = file_path
        self.degree_type = degree_type
        self._2021_col_identifier = 'avgValue'
    
    def _concat_lookup(
            self,
            df: pd.DataFrame,
            df_id: str,
            suffix: str
        ) -> pd.DataFrame:
        '''Concatenates a new dataframe with averaged columns on an old dataframe. Returns the merged dataframe.'''
        df_name = self.file_path+self.file_name+'_'+df_id+suffix+'.csv'
        try:  
            new_df = pd.read_csv(df_name, index_col = 0)
            new_df = pd.DataFrame(new_df.mean(axis = 1), columns = [self._2021_col_identifier+df_id])
        except Exception as e:
            print(f'Warning: {df_name} could not be read": {e}')
            return df
        return df.merge(new_df, how='left', left_index=True, right_index=True).fillna(0)

    def _load_dataset(self) -> pd.DataFrame:
        '''
        Loads into memory all the datasets from the filepath specified concatenated. If any is missing, it will be ignored and
        the function will try to concatenate the others. 
        '''
        match(self.degree_type):
            case DegreeType.IN_DEGREE:
                suffix = '_in'
            case DegreeType.OUT_DEGREE:
                suffix = '_out'
            case DegreeType.DEGREE:
                suffix = ''
            case _:
                raise ValueError('Degree type not recognized.')

        return (
            pd.read_csv(self.file_path+self.file_name+'_war'+suffix+'.csv', index_col = 0)
            .pipe(self._concat_lookup, df_id = '1', suffix = suffix)
            .pipe(self._concat_lookup, df_id = '2', suffix = suffix)
            .pipe(self._concat_lookup, df_id = '3', suffix = suffix)
            .pipe(self._concat_lookup, df_id = '4', suffix = suffix)
        )
    
    def _prepare_dataset(self, df: pd.DataFrame) -> None:
        '''Takes a dataframe obejct and transforms it into a DataFrame ready to be used in the Analyzer class.'''
        cols_2021 = []
        for column in df.columns:
            if column.startswith(self._2021_col_identifier):
                cols_2021.append(column)
                df[column] = df[column]/df['degree'] #absolute average value is converted to relative average value
        df['totalVal'] = df[cols_2021].sum(axis = 1)
        df['stdVal'] = df[cols_2021].std(axis = 1)

    def get_df(self) -> pd.DataFrame:
        '''Loads the datasets in memory, preprocesses them to be used in the Analyzer class and returns the dataframe object.'''
        df = self._load_dataset()
        self._prepare_dataset(df)        
        return df