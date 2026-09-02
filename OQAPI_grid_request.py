import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import geojson
import geopandas as gpd
import pandas as pd
import requests

# .\OQAPI_grid_request.py "road-comparison" "roads-all-highways" \
#     "Haiti_0.1deg.gpkg" "test_output_path.gpkg" 20


try:
    indicator = "/" + sys.argv[1]
    topic = sys.argv[2]
    input_geom_path = sys.argv[3]
    output_geom_path = sys.argv[4]
    max_workers = int(sys.argv[5])
except IndexError:
    print(
        "an IndexError occured. make sure to pass the following arguments: "
        "indicator, topic, input_geom_path, output_geom_path and max_workers"
    )
    sys.exit(1)

try:
    api_key = "YOUR_API_KEY"
except KeyError:
    print(
        "the OQAPI_API_KEY environment variable is not set. get a free API key at "
        "https://account.heigit.org/ and set it via `export OQAPI_API_KEY=<your-key>`"
    )
    sys.exit(1)


base_url = "https://api.heigit.org/ohsome-quality-api/v2"
endpoint = "/indicators"
url = base_url + endpoint + indicator

gdf = gpd.read_file(input_geom_path)
gdf["result_value"] = pd.Series([None] * len(gdf), dtype="float")
gdf["response_time"] = pd.Series([None] * len(gdf), dtype="float")

headers = {"accept": "application/json", "authorization": api_key}


def fetch(index, geometry):
    bpolys = geojson.Feature(geometry=geometry)
    bpolys_collection = geojson.FeatureCollection([bpolys])

    parameters = {
        "topic": topic,
        "bpolys": bpolys_collection,
    }
    for attempt in range(4):
        try:
            print(f"posting request for index {index}")
            startresponse = time.time()
            response = requests.post(url, headers=headers, json=parameters, timeout=60)
            response.raise_for_status()
            result = response.json()
        except requests.RequestException as e:
            print(f"Attempt {attempt + 1} failed at index {index}: {e}")
            if attempt < 3:
                print("Retrying...")
                time.sleep(2)
            else:
                print("Max retries reached. Skipping.")
                return index, None, None
        else:
            endresponse = time.time()
            responsetime = endresponse - startresponse
            value = result["result"][0]["result"]["value"]
            return index, value, responsetime


start = time.time()
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = [executor.submit(fetch, i, gdf.geometry.iloc[i]) for i in range(len(gdf))]

    for future in as_completed(futures):
        index, value, responsetime = future.result()
        gdf.at[index, "result_value"] = value
        gdf.at[index, "response_time"] = responsetime
        print(f"Completed index {index}: {value}")

end = time.time()
print(f"Calculation took {end - start:.2f} seconds")

gdf.to_file(output_geom_path, driver="GPKG")
