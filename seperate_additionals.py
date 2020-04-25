import pandas as pd
import dask
import dask.dataframe as dd


def main():
    remaining_df = dd.read_csv("cleaned_datasets/combined_nairobi_additional_cleaned.csv")
    df = remaining_df.compute()
    print(df.head())

    for place_name in df.place_type.unique():
        mini_df = df[df.place_type == place_name]
        mini_df.iloc[:, :].to_csv("cleaned_datasets/combined_nairobi_{0}_cleaned.csv"
                                   .format(place_name), index=False)


if __name__ == "__main__":
    main()
