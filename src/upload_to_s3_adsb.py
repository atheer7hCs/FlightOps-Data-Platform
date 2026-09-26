import logging
import os
from pathlib import Path

import boto3
from dotenv import load_dotenv


load_dotenv()


# ============================================================
# Configuration
# ============================================================

AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

LOCAL_DATA_PATH = Path(
    "./data/processed/adsb_historical_parquet"
)

S3_PREFIX = "processed/flights/historical"


# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("load_to_s3")


# ============================================================
# S3 Client
# ============================================================

def create_s3_client():
    """Create an S3 client."""

    return boto3.client(
        "s3",
        region_name=AWS_REGION,
    )


# ============================================================
# Upload
# ============================================================

def upload_file_to_s3(
    s3_client,
    local_file: Path,
) -> None:
    """Upload one Parquet file while preserving partition structure."""

    relative_path = local_file.relative_to(
        LOCAL_DATA_PATH
    )

    s3_key = (
        f"{S3_PREFIX}/{relative_path.as_posix()}"
    )

    logger.info(
        f"Uploading {relative_path} → "
        f"s3://{S3_BUCKET_NAME}/{s3_key}"
    )

    s3_client.upload_file(
        str(local_file),
        S3_BUCKET_NAME,
        s3_key,
    )


# ============================================================
# Main
# ============================================================

def run() -> None:
    """Upload partitioned Parquet dataset to S3."""

    if not AWS_REGION:
        raise ValueError(
            "AWS_REGION is not set in .env"
        )

    if not S3_BUCKET_NAME:
        raise ValueError(
            "S3_BUCKET_NAME is not set in .env"
        )

    if not LOCAL_DATA_PATH.exists():
        raise FileNotFoundError(
            f"Local data directory not found: "
            f"{LOCAL_DATA_PATH}"
        )

    s3_client = create_s3_client()

    files = sorted(
        LOCAL_DATA_PATH.rglob("*.parquet")
    )

    if not files:
        logger.warning(
            "No Parquet files found."
        )
        return

    logger.info(
        f"Found {len(files):,} Parquet files to upload."
    )

    for file in files:
        upload_file_to_s3(
            s3_client,
            file,
        )

    logger.info(
        f"Finished uploading "
        f"{len(files):,} Parquet files to S3."
    )


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":
    run()