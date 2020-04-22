## Location Intelligence
## https://maps.googleapis.com/maps/api/place/nearbysearch/json?
# location=-33.8670522,151.1957362&radius=1500&type=restaurant&keyword=cruise&key=YOUR_API_KEY

import pandas as pd
import json
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import requests
import time
import random
import os

API_KEY = 'AIzaSyAS3g6wkQGFNItDOBLp0dVTAS4YUPACLMA'
RADIUS = 5000


def create_request(latitude, longitude, type, pagetoken=-1):
    """ Pushes a request to the Google Places API
        :latitude - area in focus LATITUDE
        :longitude - LONGITUDE for the area in focus
        :type - the TYPE of places being searched..
    """
    if pagetoken == -1:
        request_link = \
            "https://maps.googleapis.com/maps/api/place/nearbysearch/json?" \
            "location={0},{1}&radius={2}" \
            "&type={3}&key={4}" \
                .format(latitude, longitude, RADIUS, type, API_KEY)
    elif pagetoken == 0:
        print("No Additional Pages serving results")
        request_link = 0
    else:
        request_link = \
            "https://maps.googleapis.com/maps/api/place/nearbysearch/json?" \
            "location={0},{1}&radius={2}" \
            "&type={3}&key={4}&pagetoken={5}" \
                .format(latitude, longitude, RADIUS, type, API_KEY, pagetoken)

    return request_link


def main():
    """
        atm, bank, library, cemetery, church, embassy,
        fire_station, gas_station, grocery_or_supermarket,
        gym, hospital, laundry, university, supermarket, spa,
        stadium, shopping_mall, secondary_school, school,
        police, restaurant, night_club, mosque, liquor_store,
        pharmacy, primary_school
    """

    # A collection of Centralized (Latitude and Longitude)
    # Sub-Counties and other Counties
    # Loop through the sub-counties, 5000 meters
    # Loop through the different places you are looking for:d

    print(os.getcwd())
    nairobi_df = pd.read_csv \
        ('nairobi_constituencies//nairobi_consituencies.csv')

    print(nairobi_df.head())
    lat_long = (nairobi_df["latitude"], nairobi_df["longitude"])

    # Change this variable to fetch any place you'd want
    place_types = ["night_club", "shopping_mall", "embassy", "gym", "library",
                   "bank", "atm", "gas_station", "primary_school"]

    for place_type in place_types:

        for k in range(0, len(nairobi_df)):

            page_token = -1
            lat = lat_long[0][k]
            long = lat_long[1][k]
            constituency = nairobi_df["constituency"][k]
            ward = nairobi_df["wards"][k]

            # Introducing Pandas DataFrame
            cols = ["constituency",
                    "ward",
                    "clat",
                    "clon",
                    "place_id",
                    "place_name",
                    "place_lat",
                    "place_long",
                    "place_type"]

            df = pd.DataFrame(columns=cols)

            # Running a total of 10 requests: Each Request 20 results = 200 results
            # Per each (lat, long) combination
            for total_requests in range(0, 10):
                request_url = create_request(lat, long, place_type, page_token)
                if request_url == 0:
                    continue

                r = requests.get(request_url)
                data = json.loads(r.text)

                for count in range(0, len(data["results"])):
                    print(
                        data["results"][count]["name"],
                        data["results"][count]["geometry"]["location"]["lat"],
                        data["results"][count]["geometry"]["location"]["lng"],
                        data["results"][count]["geometry"]["location"],
                        data["results"][count]["id"]
                        # data["results"][i]["rating"],
                        # data["results"][i]["user_ratings_total"],
                        # data["results"][i]["vicinity"]
                    )

                    df = df.append({
                        'constituency': constituency,
                        'ward': ward,
                        'clat': lat,
                        'clon': long,
                        'place_id': data["results"][count]["id"],
                        'place_name': data["results"][count]["name"],
                        'place_lat': data["results"][count]["geometry"]["location"]["lat"],
                        'place_long': data["results"][count]["geometry"]["location"]["lng"],
                        'place_type': place_type
                    }, ignore_index=True)

                time.sleep(30)  # Creating a 1 Minute Delay Process
                try:
                    if data["next_page_token"]:
                        page_token = data["next_page_token"]
                except KeyError:
                    page_token = 0

                df.to_csv(
                    "{0}_extract.csv".format(time.time()),
                    header=True
                )

        # Execute Next Stage
        os.system("python combine_all_outputs.py {0}".format(place_type))


if __name__ == '__main__':
    main()
