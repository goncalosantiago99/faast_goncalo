import pandas as pd
import numpy as np
from life_expectancy.cleaning_abc import ExpectancyPipeline

class ExpectancyJson( ExpectancyPipeline ):
    """ 
    This abstract class will contain all the functions
    that assure that the "Expectancy's" ETL will be 
    completed with sucess, regarding JSON files.
    """

    def load_data(self, path_inp: str) -> pd.DataFrame:
        """
        This function loads the JSON file stored in the received
        path and loads it into a pandas dataframe
        Args:
        path_inp - path of the JSON file

        Returns: the JSON file as a pandas dataframe
        """

        df = pd.read_json(path_inp)

        return df

    def clean_data(self, df: pd.DataFrame, country: str) -> pd.DataFrame:
        """
        This function does a full cleanse regarding the dataframe
        originated by the JSON file
        Args:
        df - tsv files as a pandas dataframe
        country - the country we want to filter the dataframe by

        Returns:
        filtered_df - the cleaned dataframe
        """

        # Filter and keep only rows where 'country' column has len = 2
        df = df[~(df['country'].str.len() != 2)]

        df_with_numeric = self.delete_non_numeric(df, "age")

        df_with_numeric['flag'] = df_with_numeric['flag'].replace("", "no_flag")

        df_with_numeric['flag_detail'] = df_with_numeric['flag_detail'].fillna("no_detail")

        data_type_set = self.set_datatypes(df_with_numeric)

        no_nan_df = data_type_set.dropna()

        # 'no_nan_df' is filtered by the country received as input
        # on the 'clean_data' function
        filtered_df = no_nan_df[no_nan_df['country'] == country]

        # reset indexes
        filtered_df = filtered_df.reset_index(drop=True)

        return filtered_df

    @staticmethod
    def delete_non_numeric(df: pd.DataFrame, monitor_column: str) -> pd.DataFrame:
        """
        This function deletes all the rows that aren't numerical regarding
        the column that is given as input
        Args:
        df - the dataframe to transformed
        monitor_column - the column that will be transformed

        Returns: 
        df - the dataframe without rows that aren't numerical regarding a
        specific column
        """

        # remove all chars that aren't numbers and '.'
        df.loc[:, monitor_column] = df[monitor_column].str.replace(r"[^\d\-+\.]", "", regex=True)

        return df

    @staticmethod
    def convert_to_float(value: str) -> float:
        """
        This function converts the input column to float and replaces 
        non-convertible values with NaN
        Args:
        value - the column to be converted to float

        Returns: 
        df - the converted column
        """
        try:
            return float(value)
        except ValueError:
            return np.nan

    def set_datatypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        This function converts the datatypes to the desired ones
        Args:
        df - the dataframe to be transformed

        Returns: 
        df - the transformed dataframe
        """

        # define a dictionary specifying the data types
        data_types = {'unit': str,
                    'sex': str,
                    'age': int,
                    'country': str,
                    'year': int,
                    'life_expectancy': str,
                    'flag' :str,
                    'flag_detail' : str
                    }

        # change data types based on the dictionary
        df = df.astype(data_types)

        # change the life_expectancy column data type where
        ## in error cases it return NaN
        df['life_expectancy'] = df['life_expectancy'].apply(self.convert_to_float)

        return df

    def save_data(self, df: pd.DataFrame, output_df_path: str) -> None:
        """
        This function saves the filtered dataframe to the data folder
        Args:
        df - the dataframe to be converted to CSV
        output_df_path - the path where the CSV file will be created
        """

        df.to_csv(output_df_path, index=False)
