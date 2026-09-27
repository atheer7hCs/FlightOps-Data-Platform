import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


# ============================================================
# Configuration
# ============================================================

INPUT_PATH = (
    "s3://flight-data-lake-2026/"
    "processed/flights/historical/"
)

OUTPUT_PATH = (
    "s3://flight-data-lake-2026/"
    "processed/flights/cleaned/"
)


# ============================================================
# Glue initialization
# ============================================================

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME"],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)


# ============================================================
# Read historical Parquet
# ============================================================

print("=" * 70)
print("FLIGHT HISTORICAL PROCESSING")
print("=" * 70)

print(f"Reading from: {INPUT_PATH}")

df = spark.read.parquet(INPUT_PATH)

print(f"Input columns: {df.columns}")

input_count = df.count()

print(f"Input rows: {input_count:,}")


# ============================================================
# Data type normalization
# ============================================================

df = (
    df
    .withColumn(
        "event_datetime",
        F.to_timestamp("event_datetime"),
    )
    .withColumn(
        "event_timestamp",
        F.col("event_timestamp").cast("long"),
    )
    .withColumn(
        "latitude",
        F.col("latitude").cast("double"),
    )
    .withColumn(
        "longitude",
        F.col("longitude").cast("double"),
    )
    .withColumn(
        "altitude_ft",
        F.col("altitude_ft").cast("double"),
    )
)


# ============================================================
# Remove invalid geographic records
# ============================================================

df = df.filter(
    F.col("icao24").isNotNull()
    & F.col("event_datetime").isNotNull()
    & F.col("latitude").between(-90, 90)
    & F.col("longitude").between(-180, 180)
)


# ============================================================
# Remove duplicate observations
# ============================================================

df = df.dropDuplicates(
    [
        "icao24",
        "event_timestamp",
    ]
)


# ============================================================
# Derived columns
# ============================================================

df = (
    df
    .withColumn(
        "event_date",
        F.to_date("event_datetime"),
    )
    .withColumn(
        "event_hour",
        F.hour("event_datetime"),
    )
    .withColumn(
        "altitude_band",
        F.when(
            F.col("altitude_ft").isNull(),
            "Unknown",
        )
        .when(
            F.col("altitude_ft") < 3000,
            "Low",
        )
        .when(
            F.col("altitude_ft") < 10000,
            "Mid",
        )
        .otherwise("Cruise"),
    )
)


# ============================================================
# Final column order
# ============================================================

df = df.select(
    "icao24",
    "registration",
    "aircraft_type",
    "description",
    "callsign",
    "squawk",
    "latitude",
    "longitude",
    "altitude_ft",
    "on_ground",
    "event_timestamp",
    "event_datetime",
    "event_date",
    "event_hour",
    "altitude_band",
    "source",
)


# ============================================================
# Output validation
# ============================================================

output_count = df.count()

print(f"Output rows: {output_count:,}")

print("Final schema:")
df.printSchema()


# ============================================================
# Write processed Parquet
# ============================================================

print(f"Writing to: {OUTPUT_PATH}")

(
    df
    .write
    .mode("overwrite")
    .partitionBy("event_date")
    .parquet(OUTPUT_PATH)
)


print("=" * 70)
print("PROCESSING COMPLETED")
print("=" * 70)

print(f"Input rows : {input_count:,}")
print(f"Output rows: {output_count:,}")
print(f"Output path: {OUTPUT_PATH}")
job.commit()
