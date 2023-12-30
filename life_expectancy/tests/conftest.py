"""Pytest configuration file"""
import pytest
import pandas as pd
from . import FIXTURES_DIR

@pytest.fixture(scope="session")
def life_expectancy_input_path() -> pd.DataFrame:
    """
    Fixture to return the path to the sampled tsv file that
    represents the eu_life_expectancy
    """
    return FIXTURES_DIR / "eu_life_expectancy_raw.tsv"

@pytest.fixture(scope="session")
def pt_life_expectancy_expected() -> pd.DataFrame:
    """Fixture to load the expected output of the cleaning script"""

    df_cleaned = pd.read_csv( FIXTURES_DIR / "pt_life_expectancy_expected.csv")

    return df_cleaned
