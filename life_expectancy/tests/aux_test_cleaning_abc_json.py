from abc import ABC, abstractmethod

class ExpectancyTestPipeline(ABC):
    """ 
    This abstract class will contain all the functions
    that assure that the "Expectancy's" ETL will be 
    testes with sucess.
    """

    @abstractmethod
    def test_load(self, life_expectancy_input_path_json) -> None:
        pass

    @abstractmethod
    def test_clean_data(self,
                        life_expectancy_input_path_json,
                        pt_life_expectancy_expected_json
                        ) -> None:
        pass

    @abstractmethod
    def test_save(self, life_expectancy_input_path_json, pt_life_expectancy_expected_json) -> None:
        pass
