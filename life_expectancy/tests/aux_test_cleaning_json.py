"""Tests for the cleaning module"""
from unittest import mock
from pathlib import Path
import pandas as pd
from life_expectancy.cleaning_json import ExpectancyJson
from life_expectancy.tests.aux_test_cleaning_abc_json import ExpectancyTestPipeline
from . import FIXTURES_DIR

class JsonTest(ExpectancyTestPipeline):
    """
    This class has the test functions
    in regards to the JSON ETL pipeline.
    """

    # create instance from ExpectancyJson
    json_instance =  ExpectancyJson()

    def test_load(self, life_expectancy_input_path_json):
        """
        Loads the data from the json file to a pandas dataframe
        """

        # load the data from the json
        df_loaded = self.json_instance.load_data(life_expectancy_input_path_json)

        # load the dataframe that was just cleaned and created
        eu_life_expectancy_actual = pd.read_json(FIXTURES_DIR / "eu_life_expectancy_raw_json.json")

        pd.testing.assert_frame_equal(
            df_loaded, eu_life_expectancy_actual
        )

    def test_clean_data(self, life_expectancy_input_path_json, pt_life_expectancy_expected_json):

        #Loads the data from the json file to a pandas dataframe
        df_loaded = self.json_instance.load_data(life_expectancy_input_path_json)

        # the region to filter the dataframe by
        region = "PT"

        # clean the data
        df_cleaned = self.json_instance.clean_data(df_loaded, region)

        pd.testing.assert_frame_equal(
            df_cleaned, pt_life_expectancy_expected_json
        )

    def test_save(self, life_expectancy_input_path_json, pt_life_expectancy_expected_json):
        # permite que qq função que crie um csv, nao o crie na realidade e
        # faça outra coisa a definir
        with mock.patch('pandas.DataFrame.to_csv') as mock_to_csv:
            # definir o que fazer nas funções to_csv
            mock_to_csv.side_effect = print("Data saved successfully.")

            # Loads the data from the tsv file to a pandas dataframe
            df_loaded = self.json_instance.load_data(life_expectancy_input_path_json)

            # the region to filter the dataframe by
            region = "PT"

            # cleans the loaded df
            df_cleaned = self.json_instance.clean_data(df_loaded, region)

            # store the cleaned df in a csv file in a specific path
            df_cleaned.to_csv(FIXTURES_DIR / "pt_life_expectancy_expected_json.csv", index=False)

            # path where the cleaned df will be stored
            out_path: Path  = FIXTURES_DIR / "pt_life_expectancy_expected_json.csv"

            # load the dataframe that was just cleaned and created
            pt_life_expectancy_actual = pd.read_csv(out_path)

            pd.testing.assert_frame_equal(
                pt_life_expectancy_actual, pt_life_expectancy_expected_json
            )
