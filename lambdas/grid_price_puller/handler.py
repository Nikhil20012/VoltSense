"""
Pulls grid electricity pricing and writes JSON to S3 (or local disk for dev).

In production: deployed as AWS Lambda, triggered by CloudWatch Events every 5 min.
Locally: run as a regular Python script.

Open-Meteo doesn't have pricing data, so this simulates realistic time-of-use
rates based on the current hour. In a real deployment, you'd pull from the
EIA Open Data API or a utility's rate schedule.

Usage:
    python lambdas/grid_price_puller/handler.py
"""

import json
import os
from datetime import datetime, timezone

ZONES = ["NSTAR", "EVERSOURCE_EAST", "NATIONAL_GRID"]

# Simulated time-of-use rates ($/kWh)
RATE_SCHEDULE = {
    "off_peak": 0.06,    # 10 PM - 6 AM
    "mid_peak": 0.10,    # 6 AM - 4 PM, 9 PM - 10 PM
    "on_peak": 0.18,     # 4 PM - 9 PM
}

LOCAL_OUTPUT_DIR = "lambdas/grid_price_puller/output"


def get_current_rate(hour: int) -> tuple:
    """Returns (price_per_kwh, is_peak_hour) based on time of day."""
    if 16 <= hour <= 20:
        return RATE_SCHEDULE["on_peak"], True
    elif 22 <= hour or hour < 6:
        return RATE_SCHEDULE["off_peak"], False
    else:
        return RATE_SCHEDULE["mid_peak"], False


def build_reading(zone_id: str, hour: int) -> dict:
    price, is_peak = get_current_rate(hour)
    # Add small variation per zone
    zone_offset = {"NSTAR": 0.0, "EVERSOURCE_EAST": 0.01, "NATIONAL_GRID": -0.005}
    adjusted_price = round(price + zone_offset.get(zone_id, 0), 4)

    return {
        "zone_id": zone_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "price_per_kwh_usd": adjusted_price,
        "is_peak_hour": is_peak,
    }


def write_to_s3(data: dict, zone_id: str):
    """Writes to local directory. In Lambda, this would write to S3."""
    now = datetime.now(timezone.utc)
    path = os.path.join(
        LOCAL_OUTPUT_DIR,
        now.strftime("%Y/%m/%d"),
        f"{zone_id}_{now.strftime('%H-%M')}.json",
    )
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f)
    return path


def lambda_handler(event=None, context=None):
    """AWS Lambda entry point."""
    now = datetime.now(timezone.utc)
    hour = now.hour
    results = []

    for zone_id in ZONES:
        try:
            reading = build_reading(zone_id, hour)
            path = write_to_s3(reading, zone_id)
            results.append({"zone": zone_id, "path": path, "status": "ok"})
            print(f"  {zone_id}: ${reading['price_per_kwh_usd']}/kWh, peak={reading['is_peak_hour']}")
        except Exception as e:
            results.append({"zone": zone_id, "status": "error", "error": str(e)})
            print(f"  {zone_id}: ERROR - {e}")

    return {"statusCode": 200, "body": results}


if __name__ == "__main__":
    print("Pulling grid pricing...")
    result = lambda_handler()
    print(f"\nDone. {len(result['body'])} zones processed.")