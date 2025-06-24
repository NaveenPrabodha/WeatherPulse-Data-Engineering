# scripts/clean_weather.py

import json
import pandas as pd
from datetime import datetime
import os

def load_raw_json(json_file_path=".data/raw/weather_data.json"):
    """Load raw weather JSON data"""
    with open(json_file_path, "r") as f:
        raw_data = json.load(f)
    return raw_data

def clean_weather_data(raw_data):
    """Clean and validate already simplified weather data"""
    df = pd.DataFrame(raw_data)

    # Add timestamp for when the data was cleaned
    df["timestamp"] = datetime.now()

    # Drop nulls or duplicates (just in case)
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)

    return df

def save_cleaned_data(df, file_prefix="cleaned_weather"):
    """Save cleaned data to CSV and JSON"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    os.makedirs("data/processed", exist_ok=True)

    df.to_csv(f"./data/processed/{file_prefix}_{timestamp}.csv", index=False)
    df.to_json(f"./data/processed/{file_prefix}_{timestamp}.json", orient="records", indent=2)
