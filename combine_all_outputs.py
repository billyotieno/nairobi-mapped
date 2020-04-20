import dask
import dask.dataframe as dd

# TODO 1: Extract Supermarkets
# TODO 2: (Data Cleaning) Identification and Removal of Duplicates

def main():
    # Read all files to a Dask DataFrame
    hospice_df = dd.read_csv("*.csv")
    df = hospice_df.compute()
    df = df.iloc[:, 1:]
    df.to_csv("consolidated\combined_nairobi_supermarkets.csv", index=False)


if __name__ == "__main__":
    main()
