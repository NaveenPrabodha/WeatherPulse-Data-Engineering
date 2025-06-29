import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

def load_cleaned_csv(file_path="data/processed/cleaned_weather.csv"):
    df = pd.read_csv(file_path)
    return df

def connect_postgres():
    load_dotenv()  # Load from .env
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    db = os.getenv("POSTGRES_DB")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    return engine

def store_to_postgres(df, table_name="weather_data"):
    engine = connect_postgres()
    df.to_sql(table_name, con=engine, if_exists="append", index=False)
    print(f"✅ Data inserted into table: {table_name}")

if __name__ == "__main__":
    df = load_cleaned_csv()
    print(df.head())  # optional debug
    store_to_postgres(df)
