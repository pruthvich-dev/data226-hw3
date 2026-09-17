from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import requests


@task
def extract(latitude, longitude, past_days=60):
    """Call the Open-Meteo historical weather API and return the raw JSON payload."""
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "past_days": past_days,
        "forecast_days": 0, # only past weather
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "weather_code",
        ],
        "timezone": "America/Los_Angeles",
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()


@task
def transform(payload, latitude, longitude):
    """Turn the API payload into a list of tuples matching the raw table schema:
    (latitude, longitude, date, temp_max, temp_min, precipitation, weather_code)
    """
    daily = payload["daily"]
    records = []
    for i, day in enumerate(daily["time"]):
        records.append((
            latitude,
            longitude,
            day,
            daily["temperature_2m_max"][i],
            daily["temperature_2m_min"][i],
            daily["precipitation_sum"][i],
            daily["weather_code"][i],
        ))
    return records


def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def load(records, table_name):
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")

        # Step 4: create the table under the raw schema if it doesn't exist yet
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                latitude FLOAT,
                longitude FLOAT,
                date DATE,
                temp_max FLOAT,
                temp_min FLOAT,
                precipitation FLOAT,
                weather_code INT,
                PRIMARY KEY (latitude, longitude, date)
            );
        """)

        # Step 5: delete all existing records (full refresh)
        cur.execute(f"DELETE FROM {table_name};")

        # Step 6: populate the table with the freshly extracted records via INSERT
        insert_sql = f"""
            INSERT INTO {table_name}
            (latitude, longitude, date, temp_max, temp_min, precipitation, weather_code)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cur.executemany(insert_sql, records)

        cur.execute("COMMIT;")
        print(f"Loaded {len(records)} records into {table_name}")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print("Transaction rolled back due to:", e)
        raise
    finally:
        cur.close()


with DAG(
    dag_id='WeatherToSnowflake',
    start_date=datetime(2026, 9, 1),
    catchup=False,
    schedule='30 2 * * *',
) as dag:
    latitude = float(Variable.get("latitude"))
    longitude = float(Variable.get("longitude"))
    payload = extract(latitude, longitude)
    records = transform(payload, latitude, longitude)
    load(records, "raw.weather_data_hw")

