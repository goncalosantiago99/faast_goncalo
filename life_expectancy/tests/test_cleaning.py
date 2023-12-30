"""Tests for the cleaning module"""
#from unittest import mock
from pathlib import Path
import pandas as pd
from life_expectancy.cleaning import load_data, clean_data, save_data
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

    #df_cleaned = df_cleaned.reset_index(drop=True)
    #pt_life_expectancy_expected = pt_life_expectancy_expected.reset_index(drop=True)

    pd.testing.assert_frame_equal(
        df_cleaned, pt_life_expectancy_expected
    )

def test_save(life_expectancy_input_path, pt_life_expectancy_expected):

    #Loads the data from the tsv file to a pandas dataframe
    df_loaded = load_data(life_expectancy_input_path)

    # the region to filter the dataframe by
    region = "PT"

    # cleans the loaded df
    df_cleaned = clean_data(df_loaded, region)

    # path where the cleaned df will be stored
    out_path: Path  = OUTPUT_DIR / "pt_life_expectancy.csv"

    # store the cleaned df in a csv file in a specific path
    save_data(df_cleaned, out_path)

    # load the dataframe that was just cleaned and created
    pt_life_expectancy_actual = pd.read_csv(out_path)

    pd.testing.assert_frame_equal(
        pt_life_expectancy_actual, pt_life_expectancy_expected
    )
