import pandas as pd

def transform_aqi_data(df):

    print("Starting transformation...")

    # Drop rows where AQI is missing entirely
    before = len(df)
    df = df.dropna(subset=["aqi"])
    print(f"  Dropped {before - len(df)} rows with missing AQI")

    # Fix data types
    df["aqi"]         = pd.to_numeric(df["aqi"],         errors="coerce")
    df["pm25"]        = pd.to_numeric(df["pm25"],        errors="coerce")
    df["pm10"]        = pd.to_numeric(df["pm10"],        errors="coerce")
    df["no2"]         = pd.to_numeric(df["no2"],         errors="coerce")
    df["co"]          = pd.to_numeric(df["co"],          errors="coerce")
    df["hour_of_day"] = pd.to_numeric(df["hour_of_day"], errors="coerce")
    df["fetched_at"]  = pd.to_datetime(df["fetched_at"])
    df["date"]        = pd.to_datetime(df["date"]).dt.date

    # Flag outliers before anything else
    df["is_outlier"] = df["aqi"].apply(lambda x: True if x > 500 else False)
    outlier_count    = df["is_outlier"].sum()
    if outlier_count > 0:
        print(f"  ⚠️  {outlier_count} outlier(s) detected (AQI > 500) — flagged, not removed")

    # Preserve raw AQI and cap working AQI at 500
    df["aqi_raw"] = df["aqi"].copy()
    df["aqi"]     = df["aqi"].clip(upper=500)

    # AQI category using official US EPA scale
    def classify_aqi(aqi):
        if   aqi <= 50:  return "Good"
        elif aqi <= 100: return "Moderate"
        elif aqi <= 150: return "Unhealthy for Sensitive Groups"
        elif aqi <= 200: return "Unhealthy"
        elif aqi <= 300: return "Very Unhealthy"
        else:            return "Hazardous"

    df["aqi_category"] = df["aqi"].apply(classify_aqi)

    # Severity score for correct dashboard sort order
    severity_map = {
        "Good":                           1,
        "Moderate":                       2,
        "Unhealthy for Sensitive Groups": 3,
        "Unhealthy":                      4,
        "Very Unhealthy":                 5,
        "Hazardous":                      6
    }
    df["aqi_severity"] = df["aqi_category"].map(severity_map)

    # Health advisory message
    advisory_map = {
        "Good":                           "No precautions needed",
        "Moderate":                       "Unusually sensitive people should limit outdoor activity",
        "Unhealthy for Sensitive Groups": "Sensitive groups should reduce outdoor activity",
        "Unhealthy":                      "Everyone should limit prolonged outdoor activity",
        "Very Unhealthy":                 "Avoid outdoor activity if possible",
        "Hazardous":                      "Health emergency — avoid all outdoor activity"
    }
    df["health_advisory"] = df["aqi_category"].map(advisory_map)

    # Fill missing pollutant values with column mean
    for col in ["pm25", "pm10", "no2", "co"]:
        col_mean = round(df[col].mean(), 1)
        missing  = df[col].isna().sum()
        df[col]  = df[col].fillna(col_mean)
        if missing > 0:
            print(f"  Filled {missing} missing values in '{col}' with mean ({col_mean})")

    # Round pollutant columns
    for col in ["pm25", "pm10", "no2", "co"]:
        df[col] = df[col].round(1)

    # Standardize text columns
    df["city"]        = df["city"].str.strip().str.title()
    df["region"]      = df["region"].str.strip()
    df["time_of_day"] = df["time_of_day"].str.strip()

    # Remove accidental duplicate rows
    before = len(df)
    df = df.drop_duplicates(subset=["city", "fetched_at"])
    dupes  = before - len(df)
    if dupes > 0:
        print(f"  Removed {dupes} duplicate row(s)")

    # Sort cleanly before loading
    df = df.sort_values(["region", "city", "fetched_at"]).reset_index(drop=True)

    print(f"Transformation complete!")
    print(f"  Total cities processed: {len(df)}")
    return df


if __name__ == "__main__":
    from extract import fetch_aqi_data

    raw_df   = fetch_aqi_data()
    clean_df = transform_aqi_data(raw_df)

    print("\n--- Transformed Data Preview ---")
    print(clean_df[["city", "aqi", "aqi_category",
                     "health_advisory", "pm25"]].to_string())

    print("\n--- Outlier Check ---")
    outliers = clean_df[clean_df["is_outlier"] == True]
    if len(outliers) > 0:
        print(outliers[["city", "aqi_raw", "aqi", "aqi_category"]])
    else:
        print("No outliers this run")