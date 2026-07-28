"""
Pulls weather data from Open-Meteo API for station regions
and writes JSON to S3 (or local disk for development).

In production: deployed as AWS Lambda, triggered by CloudWatch Events every 15 min.
Locally: run as a regular Python script.

Usage:
    python lambdas/weather_puller/handler.py
"""

import json
import os
from datetime import datetime, timezone

import requests

# Station regions with representative coordinates
REGIONS = {
    "ISONE_BOSTON": {"lat": 42.36, "lon": -71.06},
    "ISONE_CAMBRIDGE": {"lat": 42.37, "lon": -71.10},
    "ISONE_SOUTH_SHORE": {"lat": 42.25, "lon": -70.95},
}

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# Local dev writes to this directory. In Lambda, this would be S3.
LOCAL_OUTPUT_DIR = "lambdas/weather_puller/output"


def fetch_weather(lat: float, lon: float) -> dict:
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": "temperature_2m,precipitation,wind_speed_10m,cloud_cover",
        "timezone": "UTC",
    }
    resp = requests.get(OPEN_METEO_URL, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def build_readings(api_response: dict, region_id: str) -> dict:
    current = api_response.get("current", {})
    return {
        "region_id": region_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature_c": current.get("temperature_2m", 0),
        "precipitation_mm": current.get("precipitation", 0),
        "wind_speed_kmh": current.get("wind_speed_10m", 0),
        "cloud_cover_pct": current.get("cloud_cover", 0),
    }


def write_to_s3(data: dict, region_id: str):
    """
    In production, this writes to S3:
        s3://voltsense-raw/weather/YYYY/MM/DD/HH-MM.json

    For local dev, writes to a local directory.
    """
    now = datetime.now(timezone.utc)
    path = os.path.join(
        LOCAL_OUTPUT_DIR,
        now.strftime("%Y/%m/%d"),
        f"{region_id}_{now.strftime('%H-%M')}.json",
    )
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f)
    return path


def lambda_handler(event=None, context=None):
    """AWS Lambda entry point. Also works as a regular function call."""
    results = []
    for region_id, coords in REGIONS.items():
        try:
            api_data = fetch_weather(coords["lat"], coords["lon"])
            reading = build_readings(api_data, region_id)
            path = write_to_s3(reading, region_id)
            results.append({"region": region_id, "path": path, "status": "ok"})
            print(f"  {region_id}: {reading['temperature_c']}C, {reading['precipitation_mm']}mm")
        except Exception as e:
            results.append({"region": region_id, "status": "error", "error": str(e)})
            print(f"  {region_id}: ERROR - {e}")

    return {"statusCode": 200, "body": results}


if __name__ == "__main__":
    print("Pulling weather data...")
    result = lambda_handler()
    print(f"\nDone. {len(result['body'])} regions processed.")