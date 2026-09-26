from pathlib import Path
import pandas as pd


# =========================
# Paths
# =========================
INPUT_FILE = Path(
    "data/processed/adsb_historical/adsb_historical_points.csv"
)

OUTPUT_DIR = Path(
    "data/processed/adsb_historical_parquet"
)

CHUNK_SIZE = 250_000


# =========================
# Main
# =========================
def main():
    print("=" * 60)
    print("ADSB Historical CSV → Partitioned Parquet")
    print("=" * 60)

    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_FILE}"
        )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    total_chunks = 0

    # Read CSV in chunks
    for chunk in pd.read_csv(
        INPUT_FILE,
        chunksize=CHUNK_SIZE,
    ):
        total_chunks += 1

        # Convert event_datetime to datetime
        chunk["event_datetime"] = pd.to_datetime(
            chunk["event_datetime"],
            format="mixed",
            utc=True,
        )

        # Create partition column
        chunk["event_date"] = chunk["event_datetime"].dt.date

        # Write each date separately
        for event_date, daily_df in chunk.groupby("event_date"):
            partition_dir = OUTPUT_DIR / f"event_date={event_date}"

            partition_dir.mkdir(
                parents=True,
                exist_ok=True,
            )

            # Unique filename for each chunk
            output_file = (
                partition_dir
                / f"part-{total_chunks:05d}.parquet"
            )

            daily_df.to_parquet(
                output_file,
                engine="pyarrow",
                index=False,
            )

            total_rows += len(daily_df)

        print(
            f"Processed chunk {total_chunks:,} | "
            f"Rows processed: {total_rows:,}"
        )

    print("\n" + "=" * 60)
    print("DONE")
    print("=" * 60)
    print(f"Chunks processed : {total_chunks:,}")
    print(f"Rows written     : {total_rows:,}")
    print(f"Output directory : {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()