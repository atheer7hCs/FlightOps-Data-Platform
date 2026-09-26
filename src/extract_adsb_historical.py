import csv
import gzip
import json
import tarfile
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DIR = PROJECT_ROOT / "data" / "raw_historical"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "adsb_historical"

BATCH_SIZE = 100_000

COLUMNS = [
    "icao24",
    "registration",
    "aircraft_type",
    "description",
    "event_timestamp",
    "event_datetime",
    "latitude",
    "longitude",
    "altitude_ft",
    "on_ground",
    "callsign",
    "squawk",
    "source",
]


def get_event_datetime(timestamp: float) -> str:
    return (
        datetime.fromtimestamp(timestamp, tz=timezone.utc)
        .isoformat()
    )


def extract_trace(trace_file):
    """
    Read one compressed trace JSON from the TAR archive
    and convert its trace points into rows.
    """

    with gzip.GzipFile(fileobj=trace_file) as gz:
        aircraft = json.load(gz)

    icao24 = aircraft.get("icao")
    registration = aircraft.get("r")
    aircraft_type = aircraft.get("t")
    description = aircraft.get("desc")
    base_timestamp = aircraft.get("timestamp")

    if not icao24 or base_timestamp is None:
        return []

    rows = []

    for point in aircraft.get("trace", []):
        if not point or len(point) < 4:
            continue

        offset = point[0]
        latitude = point[1]
        longitude = point[2]
        altitude = point[3]

        # We require a valid timestamp and geographic position.
        if offset is None or latitude is None or longitude is None:
            continue

        event_timestamp = base_timestamp + offset

        metadata = point[8] if len(point) > 8 else None

        callsign = None
        squawk = None

        if isinstance(metadata, dict):
            callsign = metadata.get("flight")
            squawk = metadata.get("squawk")

            if callsign:
                callsign = callsign.strip()

        on_ground = point[6] if len(point) > 6 else None

        rows.append([
            icao24,
            registration,
            aircraft_type,
            description,
            event_timestamp,
            get_event_datetime(event_timestamp),
            latitude,
            longitude,
            altitude,
            on_ground,
            callsign,
            squawk,
            "adsb.lol",
        ])

    return rows


def process_tar(tar_path, writer, stats):
    """
    Process one ADSB.lol TAR archive.
    """

    print(f"\nProcessing: {tar_path.name}")

    with tarfile.open(tar_path, mode="r") as tar:

        trace_members = [
            member
            for member in tar
            if member.name.startswith("./traces/")
            and member.name.endswith(".json")
        ]

        print(f"Trace files found: {len(trace_members):,}")

        for index, member in enumerate(trace_members, start=1):

            extracted = tar.extractfile(member)

            if extracted is None:
                stats["invalid_files"] += 1
                continue

            try:
                rows = extract_trace(extracted)

                for row in rows:
                    writer.writerow(row)

                stats["rows"] += len(rows)
                stats["aircraft"] += 1

            except Exception as exc:
                stats["invalid_files"] += 1
                stats["errors"].append(
                    f"{tar_path.name} -> {member.name} -> {exc}"
                )

            if index % 5_000 == 0:
                print(
                    f"Processed traces: {index:,} | "
                    f"Rows: {stats['rows']:,}"
                )


def main():

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    tar_files = sorted(RAW_DIR.glob("*.tar"))

    if not tar_files:
        raise FileNotFoundError(
            f"No .tar files found in {RAW_DIR}"
        )

    print("=" * 70)
    print("ADSB.lol Historical Extraction")
    print("=" * 70)
    print(f"Input directory : {RAW_DIR}")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"TAR files       : {len(tar_files)}")
    print()

    # A single CSV is intentionally used only as an intermediate
    # validation output. We will convert it to Parquet afterwards.
    output_csv = OUTPUT_DIR / "adsb_historical_points.csv"

    stats = {
        "rows": 0,
        "aircraft": 0,
        "invalid_files": 0,
        "errors": [],
    }

    with output_csv.open(
        mode="w",
        newline="",
        encoding="utf-8",
    ) as csv_file:

        writer = csv.writer(csv_file)

        writer.writerow(COLUMNS)

        for tar_path in tar_files:
            process_tar(
                tar_path,
                writer,
                stats,
            )

    print("\n" + "=" * 70)
    print("EXTRACTION COMPLETE")
    print("=" * 70)
    print(f"TAR files processed : {len(tar_files):,}")
    print(f"Aircraft records    : {stats['aircraft']:,}")
    print(f"Rows written        : {stats['rows']:,}")
    print(f"Invalid files       : {stats['invalid_files']:,}")
    print(f"Output CSV          : {output_csv}")

    if stats["errors"]:
        error_file = OUTPUT_DIR / "extraction_errors.log"

        error_file.write_text(
            "\n".join(stats["errors"]),
            encoding="utf-8",
        )

        print(f"Errors log          : {error_file}")


if __name__ == "__main__":
    main()