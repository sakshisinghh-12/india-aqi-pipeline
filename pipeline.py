from extract   import fetch_aqi_data
from transform import transform_aqi_data
from load      import load_to_bigquery

print("=" * 50)
print("India AQI Pipeline — Starting Run")
print("=" * 50)

raw_df   = fetch_aqi_data()
clean_df = transform_aqi_data(raw_df)
load_to_bigquery(clean_df)

print("=" * 50)
print("Pipeline Run Complete")
print("=" * 50)