"""Tests for the cleaning module"""
from pathlib import Path
import pandas as pd

from life_expectancy.cleaning import load_data, clean_data, save_data
from . import OUTPUT_DIR


def test_clean_data(pt_life_expectancy_expected):
    """Run the `clean_data` function and compare the output to the expected output"""

    inp_path: Path  = OUTPUT_DIR / "eu_life_expectancy_raw.tsv"

    out_path: Path  = OUTPUT_DIR / "pt_life_expectancy.csv"

    region = "PT"

    # load the data from the tsv
    df = load_data(inp_path)

    # clean the data
    df_cleaned = clean_data(df, region)

    # store the cleaned df in a csv file in a specific path
    save_data(df_cleaned, out_path)


    pt_life_expectancy_actual = pd.read_csv(
        OUTPUT_DIR / "pt_life_expectancy.csv"
    )

    pd.testing.assert_frame_equal(
        pt_life_expectancy_actual, pt_life_expectancy_expected
    )
