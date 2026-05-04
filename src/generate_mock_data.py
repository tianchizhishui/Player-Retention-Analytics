import os
import uuid
import random
from datetime import datetime, timedelta

import pandas as pd


RAW_DATA_DIR = "data/raw"


def ensure_dirs():
    os.makedirs(RAW_DATA_DIR, exist_ok=True)


def random_datetime(start_date, days_range):
    return start_date + timedelta(
        days=random.randint(0, days_range),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )


def generate_player_events(num_players=500, max_days=30):
    events = []
    start_date = datetime(2026, 1, 1)

    platforms = ["iOS", "Android"]
    countries = ["US", "CA", "GB", "JP", "KR"]
    sources = ["organic", "paid_social", "ad_network", "referral"]

    for player_idx in range(1, num_players + 1):
        player_id = f"player_{player_idx:05d}"
        install_time = random_datetime(start_date, 5)
        platform = random.choice(platforms)
        country = random.choice(countries)
        acquisition_source = random.choice(sources)

        events.append({
            "event_id": str(uuid.uuid4()),
            "player_id": player_id,
            "session_id": None,
            "event_time": install_time,
            "event_date": install_time.date(),
            "event_name": "install",
            "platform": platform,
            "country": country,
            "acquisition_source": acquisition_source,
            "level_num": None,
            "session_length_sec": None,
        })

        active_days = random.sample(range(0, max_days), k=random.randint(3, 15))

        for day in active_days:
            session_count = random.randint(1, 3)

            for _ in range(session_count):
                session_id = str(uuid.uuid4())
                session_start = install_time + timedelta(
                    days=day,
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59),
                )
                session_length = random.randint(60, 1800)

                events.append({
                    "event_id": str(uuid.uuid4()),
                    "player_id": player_id,
                    "session_id": session_id,
                    "event_time": session_start,
                    "event_date": session_start.date(),
                    "event_name": "session_start",
                    "platform": platform,
                    "country": country,
                    "acquisition_source": acquisition_source,
                    "level_num": None,
                    "session_length_sec": session_length,
                })

                level_count = random.randint(1, 5)

                for level in range(1, level_count + 1):
                    level_start_time = session_start + timedelta(minutes=level * 3)

                    events.append({
                        "event_id": str(uuid.uuid4()),
                        "player_id": player_id,
                        "session_id": session_id,
                        "event_time": level_start_time,
                        "event_date": level_start_time.date(),
                        "event_name": "level_start",
                        "platform": platform,
                        "country": country,
                        "acquisition_source": acquisition_source,
                        "level_num": level,
                        "session_length_sec": None,
                    })

                    if random.random() < 0.75:
                        events.append({
                            "event_id": str(uuid.uuid4()),
                            "player_id": player_id,
                            "session_id": session_id,
                            "event_time": level_start_time + timedelta(minutes=2),
                            "event_date": level_start_time.date(),
                            "event_name": "level_complete",
                            "platform": platform,
                            "country": country,
                            "acquisition_source": acquisition_source,
                            "level_num": level,
                            "session_length_sec": None,
                        })

    return pd.DataFrame(events)


def generate_game_performance_events(player_events_df):
    performance_events = []

    app_versions = ["1.0.0", "1.1.0", "1.2.0"]
    device_models = ["iPhone 14", "iPhone 15", "Pixel 7", "Galaxy S23", "Galaxy S24"]

    sessions = player_events_df[
        player_events_df["event_name"] == "session_start"
    ][
        ["player_id", "session_id", "event_time", "event_date", "platform"]
    ]

    for _, row in sessions.iterrows():
        base_time = pd.to_datetime(row["event_time"])
        app_version = random.choice(app_versions)
        device_model = random.choice(device_models)

        startup_time = random.randint(800, 6000)
        latency_samples = random.randint(3, 8)

        performance_events.append({
            "event_id": str(uuid.uuid4()),
            "player_id": row["player_id"],
            "session_id": row["session_id"],
            "event_time": base_time,
            "event_date": row["event_date"],
            "platform": row["platform"],
            "app_version": app_version,
            "device_model": device_model,
            "event_type": "startup",
            "latency_ms": None,
            "startup_time_ms": startup_time,
            "is_crash": 0,
            "is_anr": 0,
        })

        for i in range(latency_samples):
            latency = max(50, int(random.gauss(250, 120)))

            performance_events.append({
                "event_id": str(uuid.uuid4()),
                "player_id": row["player_id"],
                "session_id": row["session_id"],
                "event_time": base_time + timedelta(seconds=i * 30),
                "event_date": row["event_date"],
                "platform": row["platform"],
                "app_version": app_version,
                "device_model": device_model,
                "event_type": "latency",
                "latency_ms": latency,
                "startup_time_ms": None,
                "is_crash": 0,
                "is_anr": 0,
            })

        if random.random() < 0.04:
            performance_events.append({
                "event_id": str(uuid.uuid4()),
                "player_id": row["player_id"],
                "session_id": row["session_id"],
                "event_time": base_time + timedelta(minutes=random.randint(1, 20)),
                "event_date": row["event_date"],
                "platform": row["platform"],
                "app_version": app_version,
                "device_model": device_model,
                "event_type": "crash",
                "latency_ms": None,
                "startup_time_ms": None,
                "is_crash": 1,
                "is_anr": 0,
            })

        if row["platform"] == "Android" and random.random() < 0.03:
            performance_events.append({
                "event_id": str(uuid.uuid4()),
                "player_id": row["player_id"],
                "session_id": row["session_id"],
                "event_time": base_time + timedelta(minutes=random.randint(1, 20)),
                "event_date": row["event_date"],
                "platform": row["platform"],
                "app_version": app_version,
                "device_model": device_model,
                "event_type": "anr",
                "latency_ms": None,
                "startup_time_ms": None,
                "is_crash": 0,
                "is_anr": 1,
            })

    return pd.DataFrame(performance_events)


def main():
    ensure_dirs()

    player_events = generate_player_events()
    game_performance_events = generate_game_performance_events(player_events)

    player_events.to_csv(f"{RAW_DATA_DIR}/player_events.csv", index=False)
    game_performance_events.to_csv(
        f"{RAW_DATA_DIR}/game_performance_events.csv",
        index=False
    )

    print("Generated raw data:")
    print(f"- {RAW_DATA_DIR}/player_events.csv: {len(player_events):,} rows")
    print(
        f"- {RAW_DATA_DIR}/game_performance_events.csv: "
        f"{len(game_performance_events):,} rows"
    )


if __name__ == "__main__":
    main()