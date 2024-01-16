from enum import Enum
import pandas as pd

def load_data(path_inp: str) -> pd.DataFrame:
    """
    This function loads the csv file stored in the received
    path and loads it into a pandas dataframe
    Args:
    path_inp - path of the csv file

    Returns: the csv file as a pandas dataframe
    """

    df_loaded = pd.read_csv(path_inp, sep=',')

    return df_loaded

# load the data from the csv
df = load_data("./life_expectancy/data/possible_regions.csv")

# extract unique values from the 'Region' column
unique_regions = df['region'].unique()

# Define the Region Enum class
class Region(Enum):
    """
    This class stores in an Enum the region possible values
    """

    AT = 'AT'
    BE = 'BE'
    BG = 'BG'
    CH = 'CH'
    CY = 'CY'
    CZ = 'CZ'
    DK = 'DK'
    EE = 'EE'
    EL = 'EL'
    ES = 'ES'
    FI = 'FI'
    FR = 'FR'
    HR = 'HR'
    HU = 'HU'
    IS = 'IS'
    IT = 'IT'
    LI = 'LI'
    LT = 'LT'
    LU = 'LU'
    LV = 'LV'
    MT = 'MT'
    NL = 'NL'
    NO = 'NO'
    PL = 'PL'
    PT = 'PT'
    RO = 'RO'
    SE = 'SE'
    SI = 'SI'
    SK = 'SK'
    DE = 'DE'
    AL = 'AL'
    IE = 'IE'
    ME = 'ME'
    MK = 'MK'
    RS = 'RS'
    AM = 'AM'
    AZ = 'AZ'
    GE = 'GE'
    TR = 'TR'
    UA = 'UA'
    BY = 'BY'
    UK = 'UK'
    XK = 'XK'
    FX = 'FX'
    MD = 'MD'
    SM = 'SM'
    RU = 'RU'

# Print all values in the enum
for member in list(Region):
    print(member.value)
