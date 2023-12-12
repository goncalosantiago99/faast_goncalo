import argparse
import pandas as pd
import numpy as np

# cria um objeto "ArgumentParser"
parser = argparse.ArgumentParser(description="Clean and process life expectancy data")

# adiciona o argumento que deseja passar à linha de comando usando
parser.add_argument("--region",
                    type=str,
                    default="PT",
                    help="Specify the region for the data to be filtered by")

# obtém o valor do argumento da linha de comando
args, _ = parser.parse_known_args()

# obtem o valor do argumento "--region"
REGION_INP = args.region

def clean_data():

    i_path = "./data/eu_life_expectancy_raw.tsv"
    o_path = "./data"

    loaded_tsv = load_tsv(i_path)

    splited_df = split_column(loaded_tsv, r"unit,sex,age,geo\time", ",")

    ordered_df = order_columns(splited_df)

    long_df = unpivot_df(ordered_df)

    df_no_space = work_space(long_df, "value")

    data_type_set = set_datatypes(df_no_space)

    no_nan_df = data_type_set.dropna()

    filtered_df = no_nan_df.loc[no_nan_df['region'] == REGION_INP]

    # run main function
    output_df_as_csv(filtered_df, o_path)

    return print("DF created")


def load_tsv(path):

    # Read the TSV file into a pandas DataFrame
    df = pd.read_csv(path, sep='\t')

    return df

def split_column(df, name_column, delimeter):

    # Split the values in the specific column based on a delimeter and expand them into new columns
    df[["unit", "sex", "age", "region"]] = df[name_column].str.split(delimeter, expand=True)

    # Drop splited column
    df = df.drop(columns=[name_column])

    return df

def order_columns(df):

    # create that identifies the columns that shoud be first
    init_col = ["unit", "sex", "age", "region"]

    # Reorder columns
    new_column_order = init_col + [col for col in df.columns if col not in init_col]
    df = df[new_column_order]

    return df

def unpivot_df(df):

    # create that identifies the columns that shoud be first
    unpivot_col = ["unit", "sex", "age", "region"]

    # unpivot the DataFrame to long format
    long_df = pd.melt(df, id_vars = unpivot_col, var_name='year', value_name='value')

    return long_df


def work_space(df, monitor_column):

    # Remove leading and trailing spaces, and convert to numeric values
    df[monitor_column] = df[monitor_column].str.strip()

    # Now removel all chars that aren't numbers and '.'
    df[monitor_column] = df[monitor_column].str.replace(r"[^\d\-+\.]", "", regex=True)

    return df

def detect_non_numeric( df , monitor_column):

    # Filter non-numeric values and store them in a new variable
    non_numeric_values = df.loc[~df[monitor_column].str.isnumeric(), monitor_column].unique()

    return non_numeric_values

# Convert the 'values' column to float and replace non-convertible values with NaN
def convert_to_float(value):
    try:
        return float(value)
    except ValueError:
        return np.nan

def set_datatypes(df):

    # Define a dictionary specifying the data types
    data_types = {'unit': str,
                  'sex': str,
                  'age': str,
                  'region': str,
                  'year': int,
                  'value': str
                  }

    # Change data types based on the dictionary
    df = df.astype(data_types)

    # Change the value column data type where in error cases it return NaN
    df['value'] = df['value'].apply(convert_to_float)

    return df

def output_df_as_csv(df, output_df_path):

    name_output_df = "/pt_life_expectancy.csv"

    # Save the filtered dataframe to the data folder
    df.to_csv(output_df_path + name_output_df, index=False)

if __name__ == "__main__": # pragma: no cover
    clean_data()
