import openmeteo_requests
import requests_cache
from retry_requests import retry
import taosws
import argparse
from datetime import datetime, timedelta
import sys

# Parse args
parser = argparse.ArgumentParser(description='Fetch weather forecast and save to TDengine.')
parser.add_argument('--lat', type=float, default=-33.0, help='Latitude for the forecast')
parser.add_argument('--lon', type=float, default=-56.0, help='Longitude for the forecast')
parser.add_argument('--quadrant', type=int, default=1, help='Quadrant for the forecast')
parser.add_argument('--day', type=str, default='', help='Day for the forecast') # yyyy-mm-dd
args = parser.parse_args()

# If no day, use today
if len(args.day) == 0:
    args.day = datetime.utcnow().strftime("%Y-%m-%d")

# Setup Openmeteo API
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)
url = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": args.lat,
    "longitude": args.lon,
    "hourly": "temperature_2m",
	"start_date": args.day,
	"end_date": args.day,
}

# Call Openmeteo API
try:
    responses = openmeteo.weather_api(url, params=params)
except Exception as e:
    print(f"Error fetching weather data: {e}")
    sys.exit(1)

# Process first location
response = responses[0]

# Process hourly data
hourly = response.Hourly()
hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

# Get time metadata to manually calculate timestamps
hourly_start = hourly.Time()
hourly_interval = hourly.Interval()

print(f"Fetched {len(hourly_temperature_2m)} rows for Lat: {args.lat}, Lon: {args.lon}")

# Connect to TDengine and insert Data
try:
    # Connect to database
    conn = taosws.connect(
        user="root",
        password="taosdata",
        host="localhost",
        port=6041
    )
    cursor = conn.cursor()

    # Create table if it doesn't exist
    cursor.execute("""
        CREATE STABLE IF NOT EXISTS netlabs.forecasts (
            ts timestamp, 
            temp float
        ) TAGS (
            quadrant int
        )
    """)

    # Insert data for each row
    for i, temp_val in enumerate(hourly_temperature_2m):
        current_ts = hourly_start + (i * hourly_interval) # Calculate timestamp manually: start + (index * interval)
        dt_object = datetime.utcfromtimestamp(current_ts) # Convert unix timestamp
        forecast_ts = dt_object.strftime('%Y-%m-%d %H:%M:%S') # Format as string
        
        tbname = f"q-{args.quadrant}"
        
        cursor.execute(f"""
            INSERT INTO netlabs.forecasts (tbname, ts, quadrant, temp) 
            VALUES ('{tbname}', '{forecast_ts}', {args.quadrant}, {temp_val})
        """)

    print("Data successfully inserted into TDengine.")
    
    cursor.close()
    conn.close()

except Exception as e:
    print(f"Database Eror: {e}")
    sys.exit(1)