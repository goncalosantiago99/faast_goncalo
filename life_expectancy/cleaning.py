import argparse
from life_expectancy.enum_class import Region
from life_expectancy.cleaning_tsv import ExpectancyTsv
from life_expectancy.cleaning_json import ExpectancyJson

def main():
    """
    This function executes the whole script program
    """

    # create "ArgumentParser" object
    parser = argparse.ArgumentParser(description="Clean and process life expectancy data")

    # add new command line parameter with choices from the Region Enum
    parser.add_argument("--format_input_file",
                        type=str,
                        help="Specify the format of the input fyle: csv or json")

    # add new command line parameter with choices from the Region Enum
    parser.add_argument("--region",
                        type=Region,
                        choices=list(Region),
                        default=Region.PT,
                        help="Specify the region for the data to be filtered by")

    # add new command line parameter
    parser.add_argument("--input_path",
                        type=str,
                        help="Specify the path for the tsv file that needs cleaning")

    # add new command line parameter
    parser.add_argument("--output_path",
                        type=str,
                        help="Specify the path where the cleaned csv file will be stored")

    # get the parameters value from the command lind
    args = parser.parse_args()

    # store the parameters values
    format_file = args.format_input_file
    region_inp = args.region.value
    path_inp = args.input_path
    path_out = args.output_path

    if format_file == "tsv":

         # create instance from class
        tsv_instance = ExpectancyTsv()

        # load the data from the tsv
        df = tsv_instance.load_data(path_inp)

        # clean the data
        df_cleaned = tsv_instance.clean_data(df, region_inp)

        # store the cleaned df in a csv file in a specific path
        tsv_instance.save_data(df_cleaned, path_out)

    else:

        # create instance from class
        json_instance = ExpectancyJson()

        # load the data from the tsv
        df = json_instance.load_data(path_inp)

        # clean the data
        df_cleaned = json_instance.clean_data(df, region_inp)

        # store the cleaned df in a csv file in a specific path
        json_instance.save_data(df_cleaned, path_out)

if __name__ == "__main__": # pragma: no cover
    main()
