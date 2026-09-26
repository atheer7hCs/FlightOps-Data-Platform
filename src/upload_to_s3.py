
import logging
import os
from pathlib import Path

import boto3
from dotenv import load_dotenv

load_dotenv()

# Configuration
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

LOCAL_DATA_PATH = Path("./data/raw_historical")
S3_PREFIX = "raw/flights/historical"

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("load_to_s3")


def create_s3_client():
    """Create an S3 client using AWS credentials from environment variables."""

    return boto3.client(
        "s3",
        region_name=AWS_REGION,
    )


def upload_file_to_s3(
    s3_client,
    local_file: Path,
) -> None:
    """Upload one local JSON file to the S3 raw layer."""

    s3_key = f"{S3_PREFIX}/{local_file.name}"

    logger.info(
        f"Uploading {local_file.name} → s3://{S3_BUCKET_NAME}/{s3_key}"
    )

    s3_client.upload_file(
        str(local_file),
        S3_BUCKET_NAME,
        s3_key,
    )

    logger.info(
        f"Successfully uploaded: {local_file.name}"
    )


def run() -> None:
    """Upload all historical flight batches to S3."""

    if not AWS_REGION:
        raise ValueError("AWS_REGION is not set in .env")

    if not S3_BUCKET_NAME:
        raise ValueError("S3_BUCKET_NAME is not set in .env")

    if not LOCAL_DATA_PATH.exists():
        raise FileNotFoundError(
            f"Local data directory not found: {LOCAL_DATA_PATH}"
        )

    s3_client = create_s3_client()

    files = sorted(
        LOCAL_DATA_PATH.glob("*.json")
    )

    if not files:
        logger.warning(
            "No JSON files found in the local data directory."
        )
        return

    logger.info(
        f"Found {len(files)} JSON files to upload."
    )

    for file in files:
        upload_file_to_s3(
            s3_client,
            file,
        )

    logger.info(
        f"Finished uploading {len(files)} files to S3."
    )


if __name__ == "__main__":
    run()

