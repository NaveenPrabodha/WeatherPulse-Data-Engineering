import json
import pandas as pd
from datetime import datetime
import os

def load_raw_json():
    """Load latest raw JSON file from data/raw/"""
    import glob
    raw_files = sorted(glob.glob("data/raw/weather_*.json"))
    if not raw_files:
        print("❌ No raw JSON files found.")
        return []

    latest_file = raw_files[-1]
    with open(latest_file, "r") as f:
        data = json.load(f)

    print(f"📥 Loaded raw data from: {latest_file}")
    return data

def clean_weather_data(raw_data):
    """Clean and normalize the raw weather data"""
    df = pd.DataFrame(raw_data)

    # Normalize city names
    df["city"] = df["city"].str.strip().str.title()

    # Drop missing or bad values
    df = df.dropna(subset=["temp_C", "humidity", "wind_speed", "city"])
    df["temp_C"] = pd.to_numeric(df["temp_C"], errors="coerce")
    df["humidity"] = pd.to_numeric(df["humidity"], errors="coerce")
    df["wind_speed"] = pd.to_numeric(df["wind_speed"], errors="coerce")
    df.dropna(inplace=True)

    # Remove duplicates
    df.drop_duplicates(inplace=True)

    # Add current timestamp
    df["timestamp"] = datetime.now()

    return df

def save_cleaned_data(df, file_prefix="cleaned_weather"):
    """Append cleaned data to CSV and JSON or create files if missing"""
    os.makedirs("data/processed", exist_ok=True)

    csv_path = f"data/processed/{file_prefix}.csv"
    json_path = f"data/processed/{file_prefix}.json"

    # Convert timestamp for JSON
    df["timestamp"] = df["timestamp"].astype(str)

    # CSV: append or create
    if os.path.exists(csv_path):
        df.to_csv(csv_path, mode='a', header=False, index=False)
    else:
        df.to_csv(csv_path, index=False)

    # JSON: load old → append → save
    if os.path.exists(json_path):
        with open(json_path, "r") as f:
            existing_data = json.load(f)
        combined_data = existing_data + df.to_dict(orient="records")
    else:
        combined_data = df.to_dict(orient="records")

    with open(json_path, "w") as f:
        json.dump(combined_data, f, indent=2)

    print(f"✅ Data saved to: {csv_path}")
    print(f"✅ Data saved to: {json_path}")

# 🧠 DON'T FORGET THIS AGAIN
if __name__ == "__main__":
    raw_data = load_raw_json()
    if raw_data:
        cleaned_df = clean_weather_data(raw_data)
        print(cleaned_df)  # for debug
        save_cleaned_data(cleaned_df)
