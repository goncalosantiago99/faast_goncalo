from abc import ABC, abstractmethod
import pandas as pd

class ExpectancyPipeline(ABC):
    """ 
    This abstract class will contain all the functions
    that assure that the "Expectancy's" ETL will be 
    completed with sucess.
    """

    @abstractmethod
    def load_data(self, path_inp) -> pd.DataFrame:
        pass

    @abstractmethod
    def clean_data(self, df: pd.DataFrame, country: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def save_data(self, df: pd.DataFrame, output_df_path: str) -> None:
        pass
