"""Tests for the cleaning module"""
from unittest import mock
from pathlib import Path
import pandas as pd
from life_expectancy.cleaning import load_data, clean_data
from . import OUTPUT_DIR, FIXTURES_DIR

def test_load(life_expectancy_input_path):
    """
    Loads the data from the tsv file to a pandas dataframe
    """

    # load the data from the tsv
    df_loaded = load_data(life_expectancy_input_path)

    # load the dataframe that was just cleaned and created
    eu_life_expectancy_actual = pd.read_csv(
        FIXTURES_DIR / "eu_life_expectancy_raw.tsv",
        sep='\t'
    )

    pd.testing.assert_frame_equal(
        df_loaded, eu_life_expectancy_actual
    )

def test_clean_data(life_expectancy_input_path, pt_life_expectancy_expected):

    #Loads the data from the tsv file to a pandas dataframe
    df_loaded = load_data(life_expectancy_input_path)

    # the region to filter the dataframe by
    region = "PT"

    # clean the data
    df_cleaned = clean_data(df_loaded, region)

    pd.testing.assert_frame_equal(
        df_cleaned, pt_life_expectancy_expected
    )

def test_save(life_expectancy_input_path, pt_life_expectancy_expected):
    # permite que qq função que crie um csv, nao o crie na realidade e
    # faça outra coisa a definir
    with mock.patch('pandas.DataFrame.to_csv') as mock_to_csv:
        # definir o que fazer nas funções to_csv
        mock_to_csv.side_effect = print("Data saved successfully.")

        # Loads the data from the tsv file to a pandas dataframe
        df_loaded = load_data(life_expectancy_input_path)

        # the region to filter the dataframe by
        region = "PT"

        # cleans the loaded df
        df_cleaned = clean_data(df_loaded, region)

        # store the cleaned df in a csv file in a specific path
        df_cleaned.to_csv(OUTPUT_DIR / "pt_life_expectancy.csv", index=False)

        # path where the cleaned df will be stored
        out_path: Path  = OUTPUT_DIR / "pt_life_expectancy.csv"

        # load the dataframe that was just cleaned and created
        pt_life_expectancy_actual = pd.read_csv(out_path)

        pd.testing.assert_frame_equal(
            pt_life_expectancy_actual, pt_life_expectancy_expected
        )
