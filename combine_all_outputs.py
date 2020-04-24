import dask
import dask.dataframe as dd
import os
import sys
import shutil
import glob
from LocationSearch.clean_data.remove_duplicates import *
# from nairobi-mapped.clean_data.remove_duplicates import *


def consolidate_data_extracts(extracts_file_path="*.csv", output_file_name="default"):
    """
    Function: combines all the data extracts into a single CSV and returns a tuple of FILE_PATH
    and FILE_NAME
        args
            : extracts_file_path: the file path to the csv extracts
            : output_file_name: file_name to save the final consolidated file
        returns
            : tuple of consolidated csv file path and output_file_name
    """
    ddf = dd.read_csv(extracts_file_path)
    df = ddf.compute()
    df = df.iloc[:, 1:]
    df.to_csv("consolidated/{0}.csv".format(output_file_name), index=False)

    # create the file copying directory
    os.mkdir(output_file_name.replace("combined_", ""))

    # copy all the extracted files to the created folder
    for extracted_file in glob.iglob(os.path.join(".", "*.csv")):
        shutil.move(extracted_file, output_file_name.replace("combined_", ""))

    return "consolidated/{0}.csv".format(output_file_name), output_file_name


def main():
    """ The main function consolidates file combination and phase 1 data cleaning using Harvesine
    distance"""
    # Get file_name
    file_name = sys.argv[1]
    # Test Function (consolidate_data_extracts)
    df, file_name = read_file(
        *consolidate_data_extracts(output_file_name="combined_nairobi_"+file_name)
    )
    df = add_distance_column(df)
    df = remove_duplicate_places(df)
    df.to_csv("cleaned_datasets/" + file_name + "_cleaned.csv", index=False)


if __name__ == "__main__":
    main()
