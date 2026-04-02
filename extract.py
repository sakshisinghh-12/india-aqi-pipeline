import requests
import pandas as pd
from datetime import datetime

API_TOKEN = "d1f24bb21886d00917ec4e9461cfd0c1a72c8976"

CITIES = [
    "delhi", "lucknow", "kanpur", "agra", "varanasi",
    "patna", "meerut", "ghaziabad", "noida", "faridabad",
    "mumbai", "pune", "ahmedabad", "nagpur", "nashik",
    "bangalore", "chennai", "hyderabad", "kochi", "coimbatore",
    "kolkata", "jaipur", "bhopal", "chandigarh"
]

def get_region(city):
    north   = ["delhi", "lucknow", "kanpur", "agra", "varanasi",
               "patna", "meerut", "ghaziabad", "noida", "faridabad"]
    west    = ["mumbai", "pune", "ahmedabad", "nagpur", "nashik"]
    south   = ["bangalore", "chennai", "hyderabad", "kochi", "coimbatore"]
    east    = ["kolkata"]
    central = ["jaipur", "bhopal", "chandigarh"]

    if city in north:   return "North"
    if city in west:    return "West"
    if city in south:   return "South"
    if city in east:    return "East"
    if city in central: return "Central/NW"
    return "Other"

def get_time_of_day(hour):
    """
    Assigns IST hour to a 4-hour time bucket.

    00:00 - 03:59  →  Late Night
    04:00 - 07:59  →  Early Morning
    08:00 - 11:59  →  Morning
    12:00 - 15:59  →  Afternoon
    16:00 - 19:59  →  Evening
    20:00 - 23:59  →  Night
    """
    if   0  <= hour < 4:  return "Late Night"
    elif 4  <= hour < 8:  return "Early Morning"
    elif 8  <= hour < 12: return "Morning"
    elif 12 <= hour < 16: return "Afternoon"
    elif 16 <= hour < 20: return "Evening"
    else:                 return "Night"

def fetch_aqi_data():
    all_records = []
    now = datetime.now()

    for city in CITIES:
        url      = f"https://api.waqi.info/feed/{city}/?token={API_TOKEN}"
        response = requests.get(url)
        data     = response.json()

        if data["status"] == "ok":
            record = {
                "city":               city.capitalize(),
                "region":             get_region(city),
                "aqi":                data["data"]["aqi"],
                "dominant_pollutant": data["data"]["dominentpol"],
                "pm25":  data["data"]["iaqi"].get("pm25", {}).get("v", None),
                "pm10":  data["data"]["iaqi"].get("pm10", {}).get("v", None),
                "no2":   data["data"]["iaqi"].get("no2",  {}).get("v", None),
                "co":    data["data"]["iaqi"].get("co",   {}).get("v", None),
                "fetched_at":  now.strftime("%Y-%m-%d %H:%M:%S"),
                "date":        now.strftime("%Y-%m-%d"),
                "hour_of_day": now.hour,
                "time_of_day": get_time_of_day(now.hour)
            }
            all_records.append(record)
            print(f"✅ Fetched: {city.capitalize()} — AQI: {record['aqi']} | {record['time_of_day']} | {record['region']}")
        else:
            print(f"❌ Failed: {city}")

    df = pd.DataFrame(all_records)
    return df

if __name__ == "__main__":
    df = fetch_aqi_data()
    print(f"\n--- Preview ({len(df)} cities fetched) ---")
    print(df[["city", "region", "aqi", "time_of_day"]])