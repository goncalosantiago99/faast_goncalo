import argparse
import pandas as pd
import numpy as np

def clean_data( df: pd.DataFrame, region: str) -> pd.DataFrame:
    """
    This function does a full cleanse regarding the dataframe
    originated by the TST file
    """

    splited_df = split_column(df, r"unit,sex,age,geo\time", ",")

    ordered_df = order_columns(splited_df)

    long_df = pivot_df(ordered_df)

    df_without_space = work_spaces(long_df, "value")

    df_with_numeric = delete_non_numeric(df_without_space, "value")

    data_type_set = set_datatypes(df_with_numeric)

    no_nan_df = data_type_set.dropna()

    # 'no_nan_df' is filtered by the region received as input
    # on the 'clean_data' function
    filtered_df = no_nan_df.loc[no_nan_df['region'] == region]

    return filtered_df

def load_data(path_inp: str) -> pd.DataFrame:
    """
    This function loads the TSV file stored in the received
    path and loads it into a pandas dataframe
    """

    df = pd.read_csv(path_inp, sep='\t')

    return df

def split_column(df: pd.DataFrame, name_column: str, delimeter: str) -> pd.DataFrame:
    """
    This function splits the values in the specific column based on a delimeter and 
    expands them into new columns. It also drops the column that was splited.
    """

    df[["unit", "sex", "age", "region"]] = df[name_column].str.split(delimeter, expand=True)

    df = df.drop(columns=[name_column])

    return df

def order_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function orders the columns in the dataframe
    """

    # create list that identifies the columns that shoud come first
    init_col = ["unit", "sex", "age", "region"]

    # reorder the datafrane columns based on the 'init_col' list
    new_column_order = init_col + [col for col in df.columns if col not in init_col]
    df = df[new_column_order]

    return df

def pivot_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function pivots the dataframe regarding the columns
    that represent a year
    """

    # create list that identifies the columns that shoud remain unpivoted
    unpivot_col = ["unit", "sex", "age", "region"]

    # pivots the dataframe to long format
    long_df = pd.melt(df, id_vars = unpivot_col, var_name='year', value_name='value')

    return long_df

def work_spaces(df: pd.DataFrame, monitor_column: str) -> pd.DataFrame:
    """
    This function deletes the spaces in the column that is given
    as input
    """

    # remove leading and trailing spaces, and convert to numeric values
    df[monitor_column] = df[monitor_column].str.strip()

    return df

def delete_non_numeric(df: pd.DataFrame, monitor_column: str) -> pd.DataFrame:
    """
    This function deletes all the rows that aren't numerical regarding
    the column that is given as input
    """

    # remove all chars that aren't numbers and '.'
    df[monitor_column] = df[monitor_column].str.replace(r"[^\d\-+\.]", "", regex=True)

    return df

def convert_to_float(value: str) -> float:
    """
    This function converts the input column to float and replaces 
    non-convertible values with NaN
    """
    try:
        return float(value)
    except ValueError:
        return np.nan

def set_datatypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function converts the datatypes to the desired ones
    """

    # define a dictionary specifying the data types
    data_types = {'unit': str,
                  'sex': str,
                  'age': str,
                  'region': str,
                  'year': int,
                  'value': str
                  }

    # change data types based on the dictionary
    df = df.astype(data_types)

    # change the value column data type where in error cases it return NaN
    df['value'] = df['value'].apply(convert_to_float)

    return df

def save_data(df: pd.DataFrame, output_df_path: str) -> None:
    """
    This function saves the filtered dataframe to the data folder
    """

    df.to_csv(output_df_path, index=False)

def main():
    """
    This function executes the whole script program
    """

    # create "ArgumentParser" object
    parser = argparse.ArgumentParser(description="Clean and process life expectancy data")

    # add new command line parameter
    parser.add_argument("--region",
                        type=str,
                        default="PT",
                        help="Specify the region for the data to be filtered by")

    # add new command line parameter
    parser.add_argument("--input_path",
                        type=str,
                        help="Specify the path for the tsv file that needs cleaning")

    # add new command line parameter
    parser.add_argument("--output_path",
                        type=str,
                        help="Specify the path where the cleaned csv file will be stored")

    # get the parameters value from the command lind
    args = parser.parse_args()

    # store the parameters values
    region_inp = args.region
    path_inp = args.input_path
    path_out = args.output_path

    # load the data from the tsv
    df = load_data(path_inp)

    # clean the data
    df_cleaned = clean_data(df, region_inp)

    # store the cleaned df in a csv file in a specific path
    save_data(df_cleaned, path_out)

if __name__ == "__main__": # pragma: no cover
    main()
