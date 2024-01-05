"""Creates Sample Data"""
import pandas as pd
from life_expectancy.cleaning import clean_data

# creates panda dataframe after a TSV file
df = pd.read_csv("./life_expectancy/data/eu_life_expectancy_raw.tsv", sep='\t')

def sample_with_pt(input_df: pd.DataFrame, sample_size: int) -> pd.DataFrame:
    """
    This function will get a df and extract a sample from it, with a size equal to
    "sample_size". However, it assures there is at least one row with from the "PT" region.
    Parameters:
    df - Loaded data from the TSV
    sample_size - Size of the sample 
    
    Return:
    sample - Dataframe that represents the sample of the input dataframe
    """
    # create a sample from the input dataframe
    sample = input_df.sample(sample_size)

    # while there is not "PT" row we will keep sampling
    while len(sample[sample[r'unit,sex,age,geo\time'].str.endswith('PT')]) == 0:
        sample = input_df.sample(sample_size)

    return sample

# sample the df
sample_df = sample_with_pt(df.copy(), 500)

# clean the sampled df
df_cleaned = clean_data(sample_df, "PT")

# store the sampled df in a tsv file in a specific path
sample_df.to_csv("./life_expectancy/tests/fixtures/eu_life_expectancy_raw.tsv",
    sep='\t',
    index=False)

# store the cleaned df in a tsv file in a specific path
df_cleaned.to_csv("./life_expectancy/tests/fixtures/pt_life_expectancy_expected.csv", index=False)
