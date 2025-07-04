import json
import geojson
import requests
import geopandas as gpd
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

base_url = "https://api.quality.ohsome.org/v1-test"
endpoint = "/indicators"
indicator = "/road-comparison"
url = base_url + endpoint + indicator

gdf = gpd.read_file("Haiti_0.1deg.gpkg")
gdf["completeness"] = pd.Series([None] * len(gdf), dtype="float")

headers = {"accept": "application/json"}


def fetch(index, geometry):
    bpolys = geojson.Feature(geometry=geometry)
    bpolys_collection = geojson.FeatureCollection([bpolys])

    parameters = {
        "topic": "roads-all-highways",
        "bpolys": bpolys_collection,
    }
    for attempt in range(4):
        try:
            print(f"posting request for index {index}")
            response = requests.post(url, headers=headers, json=parameters, timeout=60)
            response.raise_for_status()
            result = response.json()
            value = result["result"][0]["result"]["value"]
            return index, value
        except requests.RequestException as e:
            print(f"Attempt {attempt + 1} failed at index {index}: {e}")
            if attempt < 3:
                print("Retrying...")
                time.sleep(2)
            else:
                print("Max retries reached. Skipping.")
                return index, None



start = time.time()
with ThreadPoolExecutor(max_workers=50) as executor:
    futures = [executor.submit(fetch, i, gdf.geometry.iloc[i]) for i in range(len(gdf))]

    for future in as_completed(futures):
        index, value = future.result()
        gdf.at[index, "completeness"] = value
        print(f"Completed index {index}: {value}")

end = time.time()
print(f"Calculation took {end - start:.2f} seconds")

gdf.to_file("Haiti_road_comparison_0.1deg.gpkg", driver="GPKG")


