from clean_weather import load_raw_json, clean_weather_data, save_cleaned_data

def run_etl():
    raw_data = load_raw_json()
    if not raw_data:
        print("❌ No raw data to process. Exiting.")
        return

    cleaned_df = clean_weather_data(raw_data)
    print(f"🧹 Cleaned data preview:\n{cleaned_df.head()}")
    save_cleaned_data(cleaned_df)
    print("🚀 ETL process completed successfully!")

if __name__ == "__main__":
    run_etl()
