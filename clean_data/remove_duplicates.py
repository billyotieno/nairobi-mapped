import pandas as pd
import numpy as np


def haversine_vectorize(long1, lat1, long2, lat2):
    """
    Function calculates the distance between two latitudinal and longitudinal points
        args:
            long1, lat1 - First Longitude, Latitude Combination
            long2, lat2 - Second Longitude, Latitude Combination
        returns:
            distance between the two points in KM (Kilometers)
    """
    lon1, lat1, lon2, lat2 = map(np.radians, [long1, lat1, long2, lat2])
    newlon = lon2 - lon1
    newlat = lat2 - lat1

    harversine_f = np.sin(newlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(
        newlon / 2.0) ** 2
    dist = 2 * np.arcsin(np.sqrt(harversine_f))

    km = 6367 * dist  # 6367 for distance in KM | Miles use 3958

    return km


def read_file(file_path, file_name):
    """ 
    Function recieves a file_path and returns a Pandas DataFrame Object
        args:
            file_path: String value of path to the csv file
        returns:
            A Dataframe Object of the processed csv file
     """
    df = pd.read_csv(file_path, header=0)
    return df, file_name


def add_distance_column(df):
    """
    Function receives a DataFrame Object [LocationData], Calculates distance between the ward
    central point and the location of the place(hospital, bar, etc) in KM.
        args:
            df - DataFrame Object
        returns
            df - DataFrame Object
    """
    df["distance_km"] = haversine_vectorize(
        df["clon"],
        df["clat"],
        df["place_long"],
        df["place_lat"]
    )

    return df


def remove_duplicate_places(df):
    """
    Function receives a DataFrame and Removes all the duplicates based on harvesine distance between the
    Ward Coordinates and the Place Coordinates, Places with lesser distance are retained
        args:
            df - DataFrame Object
        returns:
            df - DataFrame Object
    """
    df = df.sort_values("distance_km", ascending=False).drop_duplicates('place_id', keep='last')
    return df
