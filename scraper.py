# %%
import requests
import time
import os

import pandas as pd
from tqdm import tqdm
import geopandas as gpd
import keplergl

# %%
# -------------------------------------------------- Scraper --------------------------------------------------
start_year = 2015
end_year = 2021
years = [str(x) for x in range(start_year, end_year + 1)]

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:96.0) Gecko/20100101 Firefox/96.0",
}
#%%
delay = 5
for year in years:
    print(f"Scraping year {year}")
    url = f"https://api.avalanche.ca/min/en/submissions?fromdate={year}-01-01&todate={year}-12-31&pagesize=100000"
    response = requests.get(url, headers=headers)

    min_list = response.json()["items"]["data"]
    min_df = pd.json_normalize(min_list)
    min_df.to_csv(os.path.join("data", f"min_reports_{year}.csv"), index=False)

    print(f"Sleeping for {delay} seconds before next request")
    time.sleep(delay)

# %%
# -------------------------------------------------- Data Processing --------------------------------------------------
# Get existing MIN reports we've already scraped
df_min_reports = pd.concat(
    [pd.read_csv(os.path.join("data", f"min_reports_{year}.csv")) for year in years]
)
forecast_regions = gpd.read_file("data/forecast_regions.json")

#%%
# Get MIN observation details for each available MIN
min_details = []
for min_id in tqdm(df_min_reports.id.values):
    response = requests.get(
        f"https://api.avalanche.ca/min/en/submissions/{min_id}", headers=headers
    )
    if response.status_code == 200:
        min_details.append(response.json())
    else:
        print(f"Error {response.status_code} for {min_id}")

df_min_reports_details = pd.concat(
    [
        pd.json_normalize(x["observations"]).assign(id=x["submissionID"])
        for x in min_details
    ]
)
#%%
df_min_reports_details.to_csv(os.path.join("data", "min_reports_details.csv"), index=False)

#%%
# Merge MIN reports with MIN details
df_min_reports_merged = df_min_reports.merge(df_min_reports_details, on="id")

# %%
# Get pre set up map config, load data
from min_map_config import config

min_map = keplergl.KeplerGl(height=700)
min_map.add_data(data=df_min_reports_merged.copy(), name="min_reports")
min_map.add_data(data=forecast_regions.copy(), name="forecast_regions")
min_map.config = config
min_map.save_to_html(file_name="index.html")

#%%
min_map
# %%
# Save map_1 config to a file
with open("min_map_config.py", "w") as f:
    f.write("config = {}".format(min_map.config))
