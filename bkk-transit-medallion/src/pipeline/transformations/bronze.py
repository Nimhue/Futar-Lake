"""BRONZE — raw files from the landing volume, ingested incrementally with Auto Loader.

Static GTFS: one streaming table per GTFS file. Every snapshot BKK publishes is
appended (with its snapshot id), so bronze is a full history of the timetable.
Columns stay STRING here; typing happens in silver.

Realtime: NDJSON written by the poller, read with an explicit schema. Anything
off-contract lands in _rescued_data instead of failing the update.
"""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from transit_lib.schemas import TRIP_UPDATE, VEHICLE_POSITION

LANDING = spark.conf.get("bkk.landing_path")  # noqa: F821 - `spark` is injected by the pipeline runtime

STATIC_TABLES = ["routes", "trips", "stops", "stop_times"]


def _with_file_metadata(df: DataFrame) -> DataFrame:
    return df.select(
        "*",
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_modification_time").alias("_file_modified_at"),
        F.current_timestamp().alias("_ingested_at"),
    )


def _strip_bom(df: DataFrame) -> DataFrame:
    # Some GTFS producers write a UTF-8 BOM; it ends up glued to the first header name.
    return df.toDF(*[c.lstrip("﻿").strip() for c in df.columns])


def _register_static(table: str) -> None:
    @dp.table(
        name=f"bronze_static_{table}",
        comment=f"Raw BKK GTFS {table}.txt, every published snapshot (append-only).",
        table_properties={"quality": "bronze"},
    )
    def _bronze_static():
        raw = (
            spark.readStream.format("cloudFiles")  # noqa: F821
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "false")
            .option("cloudFiles.partitionColumns", "snapshot")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("pathGlobFilter", f"{table}.txt")
            .load(f"{LANDING}/static/{table}")
        )
        return _with_file_metadata(_strip_bom(raw)).withColumnRenamed("snapshot", "snapshot_id")


for _t in STATIC_TABLES:
    _register_static(_t)


def _rt_stream(feed: str, schema) -> DataFrame:
    raw = (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("rescuedDataColumn", "_rescued_data")
        .option("pathGlobFilter", "*.json")
        .schema(schema)
        .load(f"{LANDING}/rt/{feed}")
    )
    return _with_file_metadata(raw).withColumn(
        "feed_date", F.to_date(F.timestamp_seconds("feed_timestamp"))
    )


@dp.table(
    name="bronze_rt_trip_updates",
    comment="Raw GTFS-RT TripUpdates, one row per trip entity per feed snapshot.",
    table_properties={"quality": "bronze"},
    cluster_by=["feed_date"],
)
@dp.expect("feed_timestamp_present", "feed_timestamp IS NOT NULL")
@dp.expect("on_contract", "_rescued_data IS NULL")
def bronze_rt_trip_updates():
    return _rt_stream("trip_updates", TRIP_UPDATE)


@dp.table(
    name="bronze_rt_vehicle_positions",
    comment="Raw GTFS-RT VehiclePositions, one row per vehicle per feed snapshot.",
    table_properties={"quality": "bronze"},
    cluster_by=["feed_date"],
)
@dp.expect("feed_timestamp_present", "feed_timestamp IS NOT NULL")
@dp.expect("on_contract", "_rescued_data IS NULL")
def bronze_rt_vehicle_positions():
    return _rt_stream("vehicle_positions", VEHICLE_POSITION)
