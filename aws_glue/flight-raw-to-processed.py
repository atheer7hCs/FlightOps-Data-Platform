import sys
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    LongType,
    DoubleType,
    BooleanType,
)


# ==========================================
# 1. Initialize Glue / Spark
# ==========================================

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME"],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)


# ==========================================
# 2. Define S3 paths
# ==========================================

RAW_PATH = (
    "s3://flight-data-lake-2026/"
    "raw/flights/year=2026/month=09/day=16/"
    "flights_2026-09-16T10-22-17.json"
)

PROCESSED_PATH = (
    "s3://flight-data-lake-2026/"
    "processed/states/"
)


# ==========================================
# 3. Read raw data
# ==========================================

print("=== ETL START ===")

print("Reading raw flight data from S3...")
print(f"RAW PATH: {RAW_PATH}")

df_raw = (
    spark.read
    .option("multiLine", "true")
    .json(RAW_PATH)
)

print("=== RAW DATA READ SUCCESS ===")

print("RAW schema:")
df_raw.printSchema()

print("RAW sample:")
df_raw.show(5, truncate=False)


# ==========================================
# 4. Explode the states array
# ==========================================

print("Exploding states array...")

df_states = (
    df_raw
    .select(
        F.col("time").alias("snapshot_time"),
        F.explode("states").alias("state")
    )
)

print("States exploded successfully.")

df_states.printSchema()


# ==========================================
# 5. Extract OpenSky state-vector fields
# ==========================================

print("Extracting OpenSky state-vector columns...")

df = df_states.select(

    # 0
    F.col("state")[0]
    .cast(StringType())
    .alias("icao24"),

    # 1
    F.trim(
        F.col("state")[1]
        .cast(StringType())
    ).alias("callsign"),

    # 2
    F.col("state")[2]
    .cast(StringType())
    .alias("origin_country"),

    # 3
    F.col("state")[3]
    .cast(LongType())
    .alias("time_position"),

    # 4
    F.col("state")[4]
    .cast(LongType())
    .alias("last_contact"),

    # 5
    F.col("state")[5]
    .cast(DoubleType())
    .alias("longitude"),

    # 6
    F.col("state")[6]
    .cast(DoubleType())
    .alias("latitude"),

    # 7
    F.col("state")[7]
    .cast(DoubleType())
    .alias("baro_altitude"),

    # 8
    F.col("state")[8]
    .cast(BooleanType())
    .alias("on_ground"),

    # 9
    F.col("state")[9]
    .cast(DoubleType())
    .alias("velocity"),

    # 10
    F.col("state")[10]
    .cast(DoubleType())
    .alias("true_track"),

    # 11
    F.col("state")[11]
    .cast(DoubleType())
    .alias("vertical_rate"),

    # 12
    F.col("state")[12]
    .alias("sensors"),

    # 13
    F.col("state")[13]
    .cast(DoubleType())
    .alias("geo_altitude"),

    # 14
    F.col("state")[14]
    .cast(StringType())
    .alias("squawk"),

    # 15
    F.col("state")[15]
    .cast(BooleanType())
    .alias("spi"),

    # 16
    F.col("state")[16]
    .cast(LongType())
    .alias("position_source"),

    # Original snapshot timestamp
    F.col("snapshot_time")
    .cast(LongType())
    .alias("snapshot_time")
)


# ==========================================
# 6. Remove invalid records
# ==========================================

print("Filtering invalid records...")

df = df.filter(
    F.col("icao24").isNotNull()
)


# ==========================================
# 7. Remove exact duplicates
# ==========================================

print("Removing exact duplicates...")

df = df.dropDuplicates()


# ==========================================
# 8. Add processing timestamp
# ==========================================

print("Adding processed_at timestamp...")

processed_at = datetime.now(timezone.utc)

df = df.withColumn(
    "processed_at",
    F.lit(processed_at)
)


# ==========================================
# 9. Reorder columns
# ==========================================

df = df.select(
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "sensors",
    "geo_altitude",
    "squawk",
    "spi",
    "position_source",
    "snapshot_time",
    "processed_at",
)


# ==========================================
# 10. Check processed data
# ==========================================

print("=== PROCESSED SCHEMA ===")

df.printSchema()

print("=== PROCESSED SAMPLE ===")

df.show(
    10,
    truncate=False
)

processed_count = df.count()

print(
    f"Number of processed records: "
    f"{processed_count}"
)


# ==========================================
# 11. Write processed data as Parquet
# ==========================================

print("Writing processed state-vector data to S3...")

(
    df.write
    .mode("overwrite")
    .parquet(PROCESSED_PATH)
)

print(
    "Successfully wrote processed state-vector "
    "data to S3."
)

print(f"PROCESSED PATH: {PROCESSED_PATH}")


# ==========================================
# 12. Finish Glue Job
# ==========================================

job.commit()

print("=== ETL JOB SUCCESS ===")
