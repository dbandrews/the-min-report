# %%
import logging
import time
from pathlib import Path
from urllib.parse import urljoin

import geopandas as gpd
import keplergl
import pandas as pd
import requests
from tqdm import tqdm

from min_map_config import config

BASE_URL = "https://avcan-services-api.prod.avalanche.ca/min/en/"


# %%
def get_min_reports(start_date: str, end_date: str) -> pd.DataFrame:
    """Get MIN reports between start_date and end_date
    inclusive.

    Parameters
    ----------
    start_date : str
        First date to get reports in format YYYY-MM-DD
    end_date : str
        Last date to get reports in format YYYY-MM-DD

    Returns
    -------
    pd.DataFrame
        MIN report summaries found
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:96.0) Gecko/20100101 Firefox/96.0",
    }
    url = urljoin(BASE_URL, f"submissions?fromdate={start_date}&todate={end_date}&pagesize=100000")

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for non-2xx status codes
        min_list = response.json()["items"]["data"]
        min_df = pd.json_normalize(min_list)
        return min_df
    except requests.exceptions.RequestException as e:
        print(f"Error occurred during request: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error


def get_min_report_details(min_id: str, sleep: int = 1) -> pd.DataFrame:
    """Get MIN report details for a given MIN ID.

    Parameters
    ----------
    min_id : str
        MIN ID to get details for
    sleep : int
        Number of seconds to sleep between requests

    Returns
    -------
    pd.DataFrame
        MIN report details
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:96.0) Gecko/20100101 Firefox/96.0",
    }
    url = urljoin(BASE_URL, f"submissions/{min_id}")
    time.sleep(sleep)
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for non-2xx status codes
        min_details = response.json()
        return pd.json_normalize(min_details["observations"]).assign(id=min_details["submissionID"])
    except requests.exceptions.RequestException as e:
        print(f"Error occurred during request: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error


# %%
if __name__ == "__main__":
    # Update data
    days_prior_check = 7
    sleep = 0

    logging.basicConfig(level=logging.INFO)
    df_min_reports = pd.read_csv(Path("data/min_reports.csv"))
    df_min_reports_details = pd.read_csv(Path("data/min_reports_details.csv"))

    last_scrape_date = df_min_reports.datetime.max()
    start_date = pd.to_datetime(last_scrape_date) + pd.Timedelta(days=1)
    end_date = pd.to_datetime("today") + pd.Timedelta(days=-days_prior_check)
    logging.info(f"Scraping from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    df_min_reports_new = get_min_reports(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))

    if len(df_min_reports_new) == 0:
        logging.info("No new reports found")
    else:
        df_min_reports_details_new = pd.concat(
            [get_min_report_details(x, sleep=sleep) for x in tqdm(df_min_reports_new.id.values)]
        )

        df_min_reports_combined = pd.concat([df_min_reports, df_min_reports_new], join="outer").drop_duplicates("id")
        df_min_reports_details_combined = pd.concat(
            [df_min_reports_details, df_min_reports_details_new], join="outer"
        ).drop_duplicates("id")

        df_min_reports_combined.sort_values("datetime").to_csv(Path("data/min_reports.csv"), index=False)
        df_min_reports_details_combined.to_csv(Path("data/min_reports_details.csv"), index=False)
        df_min_reports_merged = df_min_reports_combined.merge(df_min_reports_details_combined, on="id")

        # %%
        # Create map
        forecast_regions = gpd.read_file("data/forecast_regions.json")
        min_map = keplergl.KeplerGl(height=700)
        min_map.add_data(data=df_min_reports_merged.copy(), name="min_reports")
        min_map.add_data(data=forecast_regions.copy(), name="forecast_regions")
        min_map.config = config
        min_map.save_to_html(file_name="index.html")

    # %%
    # # To tweak map config, use the following code to open the map in a browser, then run following cell
    # min_map
    # # # %%
    # # # Save map_1 config to a file
    # with open("min_map_config.py", "w") as f:
    #     f.write("config = {}".format(min_map.config))
