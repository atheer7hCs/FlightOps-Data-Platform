import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
import random
import requests
import time as time_module
from dotenv import load_dotenv

load_dotenv()

OPENSKY_FLIGHTS_URL = "https://opensky-network.org/api/flights/all"

OPENSKY_TOKEN_URL = (
    "https://auth.opensky-network.org/"
    "auth/realms/opensky-network/"
    "protocol/openid-connect/token"
)

LOCAL_DATA_PATH = Path("./data/raw_historical")

OPENSKY_CLIENT_ID = os.getenv("OPENSKY_CLIENT_ID")
OPENSKY_CLIENT_SECRET = os.getenv("OPENSKY_CLIENT_SECRET")

# OpenSky allows a maximum time range of 2 hours (7200 seconds)
# between the begin and end timestamps for each request.
WINDOW_SECONDS = 2 * 60 * 60


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("extract_historical")


def get_access_token() -> str:
    """
    Request an OAuth2 access token from OpenSky.

    The token is used to authenticate API requests.
    """

    logger.info("Requesting OpenSky access token...")

    response = requests.post(
        OPENSKY_TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": OPENSKY_CLIENT_ID,
            "client_secret": OPENSKY_CLIENT_SECRET,
        },
        timeout=30,
    )

    response.raise_for_status()

    token_data = response.json()
    access_token = token_data["access_token"]

    logger.info("OpenSky access token obtained successfully.")

    return access_token


def generate_time_windows(
    num_windows: int,
    days_back_start: int = 1,
) -> list[tuple[int, int]]:
    """
    Generate a list of historical time windows.

    Each window covers two hours and is distributed across
    different days to provide more diverse historical data.

    Returns:
        A list of (begin_timestamp, end_timestamp) pairs
        in Unix timestamp format.
    """

    windows = []
    now = datetime.now(timezone.utc)

    for i in range(num_windows):

        # Move one day further back for each window
        # to distribute requests across different days.
        day_offset = days_back_start + i

        window_end = now - timedelta(days=day_offset)
        window_start = window_end - timedelta(
            seconds=WINDOW_SECONDS
        )

        begin_ts = int(window_start.timestamp())
        end_ts = int(window_end.timestamp())

        windows.append((begin_ts, end_ts))

    return windows


def fetch_historical_flights(
    begin_ts: int,
    end_ts: int,
    access_token: str,
) -> list[dict]:
    """
    Fetch all completed flights within a specified
    historical time window.
    """

    logger.info(
        f"Fetching flights from {begin_ts} to {end_ts}..."
    )

    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    response = requests.get(
        OPENSKY_FLIGHTS_URL,
        params={
            "begin": begin_ts,
            "end": end_ts,
        },
        headers=headers,
        timeout=60,
    )
    response.raise_for_status()
    flights = response.json()
    logger.info(
        f"Found {len(flights)} flights for this window."
    )

    return flights


def save_flights_batch(
    flights: list[dict],
    batch_number,
) -> Path:
    """Save a batch of flights as a single JSON file."""

    LOCAL_DATA_PATH.mkdir(
        parents=True,
        exist_ok=True,
    )

    file_path = (
        LOCAL_DATA_PATH
        / f"flights_batch_{batch_number}.json"
    )

    with open(
        file_path,
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(
            flights,
            f,
            ensure_ascii=False,
        )

    logger.info(
        f"Saved batch {batch_number} to: {file_path}"
    )

    return file_path

import random
def get_random_days_back(min_days: int = 1, max_days: int = 60) -> int:
    """It selects a random starting point in the past so that each run yields a different period."""
    return random.randint(min_days, max_days)



import time

def run(num_windows: int = 1, days_back_start: int = None) -> int:
    """
    Fetch several historical time windows and save them locally.

    The function uses OAuth2 authentication and respects
    OpenSky API rate limits.

    Returns:
        The total number of flights collected.
    """

    if not OPENSKY_CLIENT_ID or not OPENSKY_CLIENT_SECRET:
        raise ValueError(
            "OPENSKY_CLIENT_ID or OPENSKY_CLIENT_SECRET "
            "is not set in .env"
        )

    if days_back_start is None:
        days_back_start = get_random_days_back()

    unique_run_id = int(time.time())

    access_token = get_access_token()

    windows = generate_time_windows(num_windows, days_back_start=days_back_start)

    total_flights = 0

    for i, (begin_ts, end_ts) in enumerate(
        windows,
        start=1,
    ):
        try:
            flights = fetch_historical_flights(
                begin_ts,
                end_ts,
                access_token,
            )

            save_flights_batch(
                flights,
                batch_number=f"{unique_run_id}_{days_back_start}_{i}",
            )

            total_flights += len(flights)

        except requests.exceptions.RequestException as e:
            logger.error(
                f"Failed window {i}: {e}"
            )
            continue

        if i < len(windows):
            logger.info(
                "Waiting 5 seconds before the next request..."
            )
            time.sleep(5)

    logger.info(
        f"Finished fetching. "
        f"Total flights collected: {total_flights}"
    )

    return total_flights


if __name__ == "__main__":
    run(num_windows=15)
