"""Explicit bronze schemas for the realtime NDJSON landed by the ingest job.

The ingest job owns the JSON contract (see src/ingest/bkk_rt.py), so the
pipeline reads with a declared schema instead of inference. Anything that does
not fit lands in Auto Loader's _rescued_data column rather than failing the run.
"""
from pyspark.sql.types import (
    ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType,
)

STOP_TIME_UPDATE = StructType([
    StructField("stop_sequence", IntegerType()),
    StructField("stop_id", StringType()),
    StructField("arrival_time", LongType()),
    StructField("arrival_delay", IntegerType()),
    StructField("departure_time", LongType()),
    StructField("departure_delay", IntegerType()),
    StructField("schedule_relationship", IntegerType()),
])

TRIP_UPDATE = StructType([
    StructField("feed_timestamp", LongType()),
    StructField("entity_id", StringType()),
    StructField("trip_id", StringType()),
    StructField("route_id", StringType()),
    StructField("direction_id", IntegerType()),
    StructField("start_date", StringType()),
    StructField("start_time", StringType()),
    StructField("trip_schedule_relationship", IntegerType()),
    StructField("vehicle_id", StringType()),
    StructField("vehicle_label", StringType()),
    StructField("timestamp", LongType()),
    StructField("delay", IntegerType()),
    StructField("stop_time_updates", ArrayType(STOP_TIME_UPDATE)),
])

VEHICLE_POSITION = StructType([
    StructField("feed_timestamp", LongType()),
    StructField("entity_id", StringType()),
    StructField("vehicle_id", StringType()),
    StructField("vehicle_label", StringType()),
    StructField("license_plate", StringType()),
    StructField("trip_id", StringType()),
    StructField("route_id", StringType()),
    StructField("direction_id", IntegerType()),
    StructField("start_date", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("bearing", DoubleType()),
    StructField("speed", DoubleType()),
    StructField("current_stop_sequence", IntegerType()),
    StructField("stop_id", StringType()),
    StructField("current_status", IntegerType()),
    StructField("timestamp", LongType()),
])
