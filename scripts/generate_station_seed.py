"""
Generates 200 EV charging stations with realistic Boston-metro coordinates
and station attributes. Output is the CSV that dbt seeds into Snowflake
and the simulator uses for event generation.
"""

import random
import numpy as np
import pandas as pd

random.seed(42)
np.random.seed(42)

# Boston metro bounding box (roughly Cambridge to Quincy, Waltham to Revere)
LAT_MIN, LAT_MAX = 42.25, 42.45
LON_MIN, LON_MAX = -71.20, -70.95

NUM_STATIONS = 200

STATION_TYPES = {
    "highway_rest_stop": {"weight": 0.15, "kw_options": [150, 200, 250, 350], "connectors": (6, 12)},
    "urban_garage": {"weight": 0.40, "kw_options": [50, 100, 150], "connectors": (4, 8)},
    "suburban_lot": {"weight": 0.30, "kw_options": [50, 100, 150], "connectors": (4, 10)},
    "workplace": {"weight": 0.15, "kw_options": [50, 100], "connectors": (4, 8)},
}

CITIES = [
    "Boston", "Cambridge", "Somerville", "Brookline", "Newton",
    "Quincy", "Waltham", "Medford", "Malden", "Revere",
]

UTILITY_ZONES = ["NSTAR", "EVERSOURCE_EAST", "NATIONAL_GRID"]
PRICING_TIERS = ["standard", "premium", "economy"]

types_list = list(STATION_TYPES.keys())
type_weights = [STATION_TYPES[t]["weight"] for t in types_list]


def generate_station(index: int) -> dict:
    station_type = random.choices(types_list, weights=type_weights, k=1)[0]
    spec = STATION_TYPES[station_type]

    return {
        "station_id": f"STN-{index:04d}",
        "station_name": f"{random.choice(CITIES)} Charging Hub {index}",
        "latitude": round(np.random.uniform(LAT_MIN, LAT_MAX), 6),
        "longitude": round(np.random.uniform(LON_MIN, LON_MAX), 6),
        "city": random.choice(CITIES),
        "state": "MA",
        "zip_code": f"0{random.randint(2108, 2199)}",
        "grid_region": "ISONE_BOSTON",
        "utility_zone": random.choice(UTILITY_ZONES),
        "max_capacity_kw": random.choice(spec["kw_options"]),
        "total_connectors": random.randint(*spec["connectors"]),
        "pricing_tier": random.choice(PRICING_TIERS),
        "station_type": station_type,
        "install_date": f"202{random.randint(2, 5)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
    }


def main():
    stations = [generate_station(i) for i in range(NUM_STATIONS)]
    df = pd.DataFrame(stations)

    output_path = "dbt/voltsense_dbt/seeds/station_metadata.csv"
    df.to_csv(output_path, index=False)

    print(f"Generated {len(df)} stations -> {output_path}")
    print(f"\nStation type distribution:")
    print(df["station_type"].value_counts().to_string())
    print(f"\nSample rows:")
    print(df.head(3).to_string(index=False))


if __name__ == "__main__":
    main()