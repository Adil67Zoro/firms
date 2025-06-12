import os
import requests
import pandas as pd
import time
from pymongo import MongoClient
from datetime import datetime, timedelta
import pytz
from datetime import datetime, timedelta

INTERVAL_MINUTES = 60

MAP_KEY = '7db0ce61ad6b61b0f85ddd76641e1df6'
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:StrongPassword123@mongodb:27017/?authSource=admin")
DB_NAME = "fires"
COLLECTION_NAME = "Fires"
KZ_TZ = pytz.timezone('Asia/Qyzylorda')

days = INTERVAL_MINUTES // 60 + 1

urls = [
    f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{MAP_KEY}/MODIS_NRT/KAZ/{days}",
    f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{MAP_KEY}/VIIRS_NOAA20_NRT/KAZ/{days}",
    f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{MAP_KEY}/VIIRS_SNPP_NRT/KAZ/{days}",
    f"https://firms.modaps.eosdis.nasa.gov/api/country/csv/{MAP_KEY}/VIIRS_SNPP_SP/KAZ/{days}",
]

def fetch_and_upload():
    all_dataframes = []
    for url in urls:
        try:
            df = pd.read_csv(url)
            all_dataframes.append(df)
        except Exception as e:
            print(f"Failed to read from {url}: {e}", flush=True)
        
    if not all_dataframes:
        print("No data fetched from any source. Skipping MongoDB update.", flush=True)
        return

    combined_df = pd.concat(all_dataframes, ignore_index=True)
    
    if combined_df.empty:
        print("Combined dataframe is empty. Skipping MongoDB update.", flush=True)
        return

    current_kz_time = datetime.now(KZ_TZ)
    combined_df["api_requested_datetime"] = current_kz_time.strftime("%Y-%m-%dT%H:%M:%S%z")

    records = combined_df.to_dict(orient='records')

    for x in records:
        acq_date = x.get('acq_date')
        acq_time = str(x.get('acq_time')).zfill(4) 

        try:
            dt_str = acq_date + " " + acq_time[:2] + ":" + acq_time[2:]
            dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M")

            dt += timedelta(hours=5)

            iso_str = dt.strftime("%Y-%m-%dT%H:%M:%S+0500")

            x["sputnik_recorded_datetime"] = iso_str
            del x["acq_date"]
            del x["acq_time"]

        except Exception as e:
            print(f"Error processing record {x}: {e}", flush=True)


    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    unique_records = []
    all = collection.find()
    existing = set(
        (entry['latitude'], entry['longitude'], entry['sputnik_recorded_datetime'])
        for entry in all
    )

    for entry in records:
        key = (entry['latitude'], entry['longitude'], entry['sputnik_recorded_datetime'])
        if key not in existing:
            unique_records.append(entry)

    if not unique_records:
        print("No records to insert. Skipping MongoDB update.", flush=True)
        return
    
    
    for x in unique_records:
        if x.get('instrument') == "MODIS":
            del x['bright_ti4']
            del x['bright_ti5']
        elif x.get('instrument') == "VIIRS":
            del x['brightness']
            del x['bright_t31']

        del x['type']

    collection.insert_many(unique_records)

    print(f"Inserted {len(unique_records)} documents at {current_kz_time}", flush=True)



if __name__ == "__main__":
    while True:
        fetch_and_upload()
        time.sleep(600)

        