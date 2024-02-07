from life_expectancy.tests.aux_test_cleaning_json import JsonTest
from life_expectancy.tests.aux_test_cleaning_tsv import TsvTest

# create istance from JsonTest
inst_json = JsonTest()

# create istance from TsvTest
inst_tsv = TsvTest()

def test_json(life_expectancy_input_path_json, pt_life_expectancy_expected_json):
    """
    Testes all the 3 main functions in regards
    to the 'json' type file.
    """
    inst_json.test_load(life_expectancy_input_path_json)

    inst_json.test_clean_data(life_expectancy_input_path_json, pt_life_expectancy_expected_json)

    inst_json.test_save(life_expectancy_input_path_json, pt_life_expectancy_expected_json)

def test_tsv(life_expectancy_input_path_tsv, pt_life_expectancy_expected_tsv):
    """
    Testes all the 3 main functions in regards
    to the 'tsv' type file.
    """

    inst_tsv.test_load(life_expectancy_input_path_tsv)

    inst_tsv.test_clean_data(life_expectancy_input_path_tsv, pt_life_expectancy_expected_tsv)

    inst_tsv.test_save(life_expectancy_input_path_tsv, pt_life_expectancy_expected_tsv)
