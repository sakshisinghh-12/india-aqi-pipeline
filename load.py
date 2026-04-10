import os
import pandas as pd
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"

PROJECT_ID = "india-aqi-pipeline"
DATASET_ID = "aqi_data"
TABLE_ID   = "aqi_readings"

FULL_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

def load_to_bigquery(df):

    print("Connecting to BigQuery...")
    client = bigquery.Client(project=PROJECT_ID)

    # Duplicate guard
    current_date = df["date"].iloc[0]
    current_hour = int(df["hour_of_day"].iloc[0])

    check_query = f"""
        SELECT COUNT(*) as existing_rows
        FROM `{FULL_TABLE}`
        WHERE date = '{current_date}'
        AND hour_of_day = {current_hour}
    """

    try:
        result = client.query(check_query).result()
        for row in result:
            existing = row.existing_rows

        if existing > 0:
            print(f"⚠️  Skipping load — data for {current_date} hour {current_hour} already exists ({existing} rows)")
            return
    except Exception:
        pass

    job_config = bigquery.LoadJobConfig(
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND,
        schema = [
            bigquery.SchemaField("city",               "STRING"),
            bigquery.SchemaField("region",             "STRING"),
            bigquery.SchemaField("aqi",                "FLOAT"),
            bigquery.SchemaField("aqi_raw",            "FLOAT"),
            bigquery.SchemaField("dominant_pollutant", "STRING"),
            bigquery.SchemaField("pm25",               "FLOAT"),
            bigquery.SchemaField("pm10",               "FLOAT"),
            bigquery.SchemaField("no2",                "FLOAT"),
            bigquery.SchemaField("co",                 "FLOAT"),
            bigquery.SchemaField("aqi_category",       "STRING"),
            bigquery.SchemaField("aqi_severity",       "INTEGER"),
            bigquery.SchemaField("health_advisory",    "STRING"),
            bigquery.SchemaField("is_outlier",         "BOOLEAN"),
            bigquery.SchemaField("fetched_at",         "TIMESTAMP"),
            bigquery.SchemaField("date",               "DATE"),
            bigquery.SchemaField("hour_of_day",        "INTEGER"),
            bigquery.SchemaField("time_of_day",        "STRING"),
        ]
    )

    print(f"Loading {len(df)} rows into {FULL_TABLE}...")
    load_job = client.load_table_from_dataframe(df, FULL_TABLE, job_config=job_config)
    load_job.result()

    table = client.get_table(FULL_TABLE)
    print(f"✅ Successfully loaded {len(df)} rows into BigQuery!")
    print(f"   Table: {FULL_TABLE}")
    print(f"   Total rows now in table: {table.num_rows}")


if __name__ == "__main__":
    from extract   import fetch_aqi_data
    from transform import transform_aqi_data

    raw_df   = fetch_aqi_data()
    clean_df = transform_aqi_data(raw_df)
    load_to_bigquery(clean_df)
