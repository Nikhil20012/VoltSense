"""
Produces OCPP-like charging events to Kafka. Reads station metadata
from the dbt seed CSV and generates sessions with time-of-day demand
patterns that vary by station type.

Usage:
    python simulator/charger_simulator.py
"""

import json
import time
import uuid
import random
import signal
import sys
from datetime import datetime, timezone

import pandas as pd
from confluent_kafka import Producer


KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "voltsense.charger.sessions"
STATIONS_CSV = "dbt/voltsense_dbt/seeds/station_metadata.csv"
CYCLE_INTERVAL_SEC = 5  # how often the simulator loops

# Loaded at startup
stations_df = None
station_meta = {}
station_ids = []

# Tracks currently active charging sessions
active_sessions = {}


# Demand probability by hour for each station type.
# Values represent the likelihood of a new session starting per cycle.
# These are loosely modeled on UrbanEV dataset patterns.
DEMAND_CURVES = {
    "highway_rest_stop": {
        0: 0.05, 1: 0.03, 2: 0.02, 3: 0.02, 4: 0.03, 5: 0.05,
        6: 0.10, 7: 0.15, 8: 0.20, 9: 0.25, 10: 0.35, 11: 0.30,
        12: 0.25, 13: 0.30, 14: 0.35, 15: 0.30, 16: 0.25, 17: 0.35,
        18: 0.40, 19: 0.30, 20: 0.20, 21: 0.15, 22: 0.10, 23: 0.07,
    },
    "urban_garage": {
        0: 0.05, 1: 0.03, 2: 0.02, 3: 0.02, 4: 0.03, 5: 0.05,
        6: 0.10, 7: 0.15, 8: 0.25, 9: 0.30, 10: 0.30, 11: 0.30,
        12: 0.25, 13: 0.25, 14: 0.25, 15: 0.25, 16: 0.30, 17: 0.40,
        18: 0.45, 19: 0.40, 20: 0.30, 21: 0.20, 22: 0.10, 23: 0.07,
    },
    "suburban_lot": {
        0: 0.03, 1: 0.02, 2: 0.02, 3: 0.02, 4: 0.02, 5: 0.05,
        6: 0.10, 7: 0.25, 8: 0.35, 9: 0.30, 10: 0.20, 11: 0.15,
        12: 0.15, 13: 0.15, 14: 0.15, 15: 0.20, 16: 0.30, 17: 0.40,
        18: 0.45, 19: 0.35, 20: 0.25, 21: 0.15, 22: 0.08, 23: 0.05,
    },
    "workplace": {
        0: 0.01, 1: 0.01, 2: 0.01, 3: 0.01, 4: 0.01, 5: 0.02,
        6: 0.05, 7: 0.15, 8: 0.40, 9: 0.45, 10: 0.40, 11: 0.35,
        12: 0.30, 13: 0.35, 14: 0.35, 15: 0.30, 16: 0.25, 17: 0.10,
        18: 0.05, 19: 0.03, 20: 0.02, 21: 0.01, 22: 0.01, 23: 0.01,
    },
}

CONNECTOR_TYPES = ["CCS2", "CHAdeMO", "J1772", "Tesla"]


def load_stations():
    global stations_df, station_meta, station_ids
    stations_df = pd.read_csv(STATIONS_CSV)
    station_meta = stations_df.set_index("station_id").to_dict("index")
    station_ids = stations_df["station_id"].tolist()
    print(f"Loaded {len(station_ids)} stations from {STATIONS_CSV}")


def build_event(event_type: str, station_id: str, session_id: str,
                charger_id: str, power_kw: float, energy_kwh: float,
                soc_pct: int) -> dict:
    return {
        "event_type": event_type,
        "station_id": station_id,
        "charger_id": charger_id,
        "connector_type": random.choice(CONNECTOR_TYPES),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "session_id": session_id,
        "energy_kwh_cumulative": round(energy_kwh, 2),
        "power_kw": round(power_kw, 1),
        "soc_pct": soc_pct,
        "user_id_hash": uuid.uuid4().hex[:12],
    }


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")


def start_new_sessions(producer: Producer, current_hour: int) -> int:
    """Probabilistically start new charging sessions based on time of day."""
    count = 0
    sample_size = min(50, len(station_ids))

    for sid in random.sample(station_ids, sample_size):
        meta = station_meta[sid]
        curve = DEMAND_CURVES.get(meta["station_type"], DEMAND_CURVES["urban_garage"])
        probability = curve.get(current_hour, 0.1)

        # Scale down so we don't start too many sessions per cycle
        if random.random() >= probability * 0.3:
            continue

        # Pick a charger at this station
        charger_num = random.randint(1, meta["total_connectors"])
        charger_id = f"{sid}-CHG-{charger_num:02d}"
        session_id = f"sess-{uuid.uuid4().hex[:8]}"

        max_power = min(
            meta["max_capacity_kw"] / meta["total_connectors"],
            random.choice([50, 100, 150, 250]),
        )
        initial_power = max_power * random.uniform(0.7, 1.0)

        active_sessions[session_id] = {
            "station_id": sid,
            "charger_id": charger_id,
            "start_time": time.time(),
            "energy_kwh": 0.0,
            "power_kw": initial_power,
        }

        event = build_event(
            event_type="SESSION_START",
            station_id=sid,
            session_id=session_id,
            charger_id=charger_id,
            power_kw=initial_power,
            energy_kwh=0.0,
            soc_pct=random.randint(10, 50),
        )

        producer.produce(
            topic=TOPIC,
            key=sid.encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report,
        )
        count += 1

    return count


def send_heartbeats(producer: Producer) -> int:
    """Send a heartbeat for every active session."""
    count = 0

    for session_id, session in active_sessions.items():
        # Accumulate energy based on power draw and cycle interval
        energy_increment = (session["power_kw"] / 3600) * CYCLE_INTERVAL_SEC
        session["energy_kwh"] += energy_increment

        # Simulate small power fluctuations
        current_power = session["power_kw"] * random.uniform(0.90, 1.05)

        # Estimate SoC based on elapsed time
        elapsed_min = (time.time() - session["start_time"]) / 60
        soc = min(95, int(30 + (elapsed_min / 60) * 25))

        event = build_event(
            event_type="HEARTBEAT",
            station_id=session["station_id"],
            session_id=session_id,
            charger_id=session["charger_id"],
            power_kw=current_power,
            energy_kwh=session["energy_kwh"],
            soc_pct=soc,
        )

        producer.produce(
            topic=TOPIC,
            key=session["station_id"].encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report,
        )
        count += 1

    return count


def end_sessions(producer: Producer) -> int:
    """End sessions that have been charging long enough."""
    count = 0
    to_remove = []

    for session_id, session in active_sessions.items():
        elapsed_min = (time.time() - session["start_time"]) / 60

        # Sessions need at least 2 minutes before they can end
        if elapsed_min < 2:
            continue

        # Chance of ending increases with time
        end_chance = min(0.8, elapsed_min / 60)
        if random.random() >= end_chance * 0.15:
            continue

        event = build_event(
            event_type="SESSION_END",
            station_id=session["station_id"],
            session_id=session_id,
            charger_id=session["charger_id"],
            power_kw=0.0,
            energy_kwh=session["energy_kwh"],
            soc_pct=random.randint(70, 95),
        )

        producer.produce(
            topic=TOPIC,
            key=session["station_id"].encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report,
        )
        to_remove.append(session_id)
        count += 1

    for sid in to_remove:
        del active_sessions[sid]

    return count


def main():
    load_stations()

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "voltsense-charger-simulator",
    })

    # Graceful shutdown on Ctrl+C
    running = True
    def handle_signal(sig, frame):
        nonlocal running
        print("\nShutting down...")
        running = False
    signal.signal(signal.SIGINT, handle_signal)

    print(f"Producing to: {TOPIC}")
    print(f"Cycle interval: {CYCLE_INTERVAL_SEC}s")
    print("Press Ctrl+C to stop\n")

    cycle = 0
    total_events = 0

    while running:
        cycle += 1
        current_hour = datetime.now(timezone.utc).hour

        starts = start_new_sessions(producer, current_hour)
        heartbeats = send_heartbeats(producer)
        ends = end_sessions(producer)

        producer.flush()

        cycle_total = starts + heartbeats + ends
        total_events += cycle_total
        print(
            f"Cycle {cycle}: "
            f"+{starts} starts, {heartbeats} heartbeats, -{ends} ends | "
            f"Active: {len(active_sessions)} | "
            f"Total: {total_events}"
        )

        time.sleep(CYCLE_INTERVAL_SEC)

    producer.flush()
    print(f"\nStopped. Total events produced: {total_events}")


if __name__ == "__main__":
    main()