import os
from pathlib import Path

import boto3
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")

LOCAL_PREVIEW_DIR = Path("./data/preview")
LOCAL_PREVIEW_DIR.mkdir(parents=True, exist_ok=True)

def find_first_s3_key(s3_client, prefix: str, suffix: str) -> str:
    """It iterates through the first file in S3 under a
      specific prefix and with a specific extension."""
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)
    contents = response.get("Contents", [])

    for obj in contents:
        if obj["Key"].endswith(suffix):
            return obj["Key"]

    raise FileNotFoundError(f"File not found with extension {suffix} under {prefix}")


def download_from_s3(s3_client, s3_key: str, local_name: str) -> Path:
    """It downloads a single file from S3 locally for previewing."""
    local_path = LOCAL_PREVIEW_DIR / local_name
    print(f"Downloading s3://{S3_BUCKET_NAME}/{s3_key} ...")
    s3_client.download_file(S3_BUCKET_NAME, s3_key, str(local_path))
    return local_path

def preview_dataset(label: str, df: pd.DataFrame) -> None:
    """It prints a clear summary of any DataFrame — shape, columns, missing values, and a sample."""
    print("\n" + "=" * 60)
    print(f"📊 {label}")
    print("=" * 60)

    print(f"Number of rows: {df.shape[0]:,} | Number of columns: {df.shape[1]}")

    print("\nColumns and their types:")
    print(df.dtypes)

    print("\nNumber of missing values in each column:")
    print(df.isnull().sum())

    print("\nFirst 5 rows:")
    print(df.head())


def run() -> None:
    s3_client = boto3.client("s3", region_name=AWS_REGION)

    # --- the raw file (before) ---
    raw_key = find_first_s3_key(s3_client, prefix="raw/flights/historical/", suffix=".json")
    raw_local_path = download_from_s3(s3_client, raw_key, "before_raw.json")
    df_before = pd.read_json(raw_local_path)

    # --- the processed file (after) ---
    processed_key = find_first_s3_key(s3_client, prefix="processed/flights/", suffix=".parquet")
    processed_local_path = download_from_s3(s3_client, processed_key, "after_processed.parquet")
    df_after = pd.read_parquet(processed_local_path)

    preview_dataset("Before processing (Bronze - Raw)", df_before)
    preview_dataset("After processing (Silver - Glue/PySpark)", df_after)


if __name__ == "__main__":
    run()