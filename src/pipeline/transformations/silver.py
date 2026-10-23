"""SILVER — typed, deduplicated, joinable.

Static timetable: AUTO CDC (SCD type 1) keyed on the GTFS natural keys and
sequenced by snapshot id. Result: the latest known version of every route /
trip / stop / stop_time, *including* trips that dropped out of newer feeds, so
yesterday's realtime events still find their schedule.

Realtime: the poller sees the same trip ~120 times an hour, with predictions
that converge on the actual time. We keep every prediction in
silver_rt_stop_updates (useful for prediction-accuracy work) and collapse to the
last *realized* observation per stop visit in silver_stop_arrivals.
"""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from transit_lib.gtfs_time import parse_service_date
from transit_lib.transforms import enrich_arrivals, flatten_stop_time_updates, type_stop_times


def _col(df: DataFrame, name: str, dtype: str = "string"):
    """Optional GTFS column: use it if the feed has it, else typed NULL."""
    return (F.col(name) if name in df.columns else F.lit(None)).cast(dtype).alias(name)


def _snapshot_cols():
    return [
        F.col("snapshot_id").cast("long").alias("snapshot_seq"),
        F.to_date(F.substring("snapshot_id", 1, 8), "yyyyMMdd").alias("snapshot_date"),
    ]


# ---------------------------------------------------------------- static timetable (SCD1)

STATIC_SPECS = {
    # table: (keys, typed projection, drop-if-violated expectations)
    "routes": (
        ["route_id"],
        lambda d: [_col(d, "route_id"), _col(d, "route_short_name"), _col(d, "route_long_name"),
                   _col(d, "route_type", "int"), _col(d, "route_color"), _col(d, "route_desc")],
        {"route_id_present": "route_id IS NOT NULL"},
    ),
    "trips": (
        ["trip_id"],
        lambda d: [_col(d, "trip_id"), _col(d, "route_id"), _col(d, "service_id"),
                   _col(d, "direction_id", "int"), _col(d, "trip_headsign"), _col(d, "shape_id")],
        {"trip_id_present": "trip_id IS NOT NULL", "route_id_present": "route_id IS NOT NULL"},
    ),
    "stops": (
        ["stop_id"],
        lambda d: [_col(d, "stop_id"), _col(d, "stop_name"), _col(d, "stop_lat", "double"),
                   _col(d, "stop_lon", "double"), _col(d, "location_type", "int"), _col(d, "parent_station")],
        {"stop_id_present": "stop_id IS NOT NULL"},
    ),
    "stop_times": (
        ["trip_id", "stop_sequence"],
        lambda d: [_col(d, "trip_id"), _col(d, "stop_sequence"), _col(d, "stop_id"),
                   _col(d, "arrival_time"), _col(d, "departure_time")],
        {"trip_id_present": "trip_id IS NOT NULL", "stop_sequence_present": "stop_sequence IS NOT NULL",
         "has_a_time": "sched_arrival_s IS NOT NULL OR sched_departure_s IS NOT NULL"},
    ),
}

# Warn-only checks: worth seeing in the event log, not worth dropping rows for.
STATIC_WARNINGS = {
    "stops": {"in_budapest_area": "stop_lat BETWEEN 47.0 AND 48.2 AND stop_lon BETWEEN 18.5 AND 19.8"},
}


def _register_static_scd1(table: str, keys, project, expectations) -> None:
    view = f"v_{table}_typed"

    @dp.temporary_view(name=view)
    def _typed():
        src = spark.readStream.table(f"bronze_static_{table}")  # noqa: F821
        typed = src.select(*project(src), *_snapshot_cols())
        return type_stop_times(typed) if table == "stop_times" else typed

    extra = {"expect_all": STATIC_WARNINGS[table]} if table in STATIC_WARNINGS else {}
    dp.create_streaming_table(
        name=f"silver_{table}",
        comment=f"Latest version of every GTFS {table} row across all snapshots (SCD1 on {keys}).",
        table_properties={"quality": "silver"},
        expect_all_or_drop=expectations,
        **extra,
    )
    dp.create_auto_cdc_flow(
        target=f"silver_{table}",
        source=view,
        keys=keys,
        sequence_by=F.col("snapshot_seq"),
        stored_as_scd_type=1,
    )


for _name, (_keys, _proj, _exp) in STATIC_SPECS.items():
    _register_static_scd1(_name, _keys, _proj, _exp)


# ---------------------------------------------------------------- realtime

@dp.table(
    name="silver_rt_stop_updates",
    comment="Every stop-level prediction/observation from TripUpdates, one row per snapshot.",
    table_properties={"quality": "silver"},
    cluster_by=["service_date", "route_id"],
)
@dp.expect_all_or_drop({
    "trip_id_present": "trip_id IS NOT NULL",
    "service_date_valid": "service_date IS NOT NULL",
    "stop_sequence_present": "stop_sequence IS NOT NULL",
})
@dp.expect("delay_plausible", "reported_delay_s IS NULL OR abs(reported_delay_s) < 4 * 3600")
def silver_rt_stop_updates():
    return (
        flatten_stop_time_updates(spark.readStream.table("bronze_rt_trip_updates"))  # noqa: F821
        .withWatermark("feed_ts", "2 hours")
        # protects against the same snapshot file being replayed/re-landed
        .dropDuplicatesWithinWatermark(["service_date", "trip_id", "stop_sequence", "feed_epoch"])
    )


@dp.temporary_view(name="v_realized_stop_events")
def v_realized_stop_events():
    return (
        spark.readStream.table("silver_rt_stop_updates")  # noqa: F821
        .where("is_realized AND event_epoch IS NOT NULL")
        .where("coalesce(stop_schedule_relationship, 0) <> 1")  # SKIPPED stops have no arrival
        .where("coalesce(trip_schedule_relationship, 0) <> 3")  # CANCELED trips
    )


dp.create_streaming_table(
    name="silver_stop_arrivals",
    comment="One row per (service_date, trip, stop visit): the last realized time BKK reported.",
    table_properties={"quality": "silver"},
    cluster_by=["service_date"],
)
dp.create_auto_cdc_flow(
    target="silver_stop_arrivals",
    source="v_realized_stop_events",
    keys=["service_date", "trip_id", "stop_sequence"],
    sequence_by=F.col("feed_epoch"),
    stored_as_scd_type=1,
)


@dp.table(
    name="silver_vehicle_positions",
    comment="Deduplicated vehicle positions with typed timestamps.",
    table_properties={"quality": "silver"},
    cluster_by=["service_date", "route_id"],
)
@dp.expect_all_or_drop({
    "vehicle_id_present": "vehicle_id IS NOT NULL",
    "has_position": "latitude IS NOT NULL AND longitude IS NOT NULL",
})
def silver_vehicle_positions():
    return (
        spark.readStream.table("bronze_rt_vehicle_positions")  # noqa: F821
        .select(
            "vehicle_id", "vehicle_label", "license_plate", "trip_id", "route_id", "direction_id",
            parse_service_date(F.col("start_date")).alias("service_date"),
            "latitude", "longitude", "bearing", "speed",
            "current_stop_sequence", "stop_id", "current_status",
            F.timestamp_seconds(F.coalesce("timestamp", "feed_timestamp")).alias("position_ts"),
            F.timestamp_seconds("feed_timestamp").alias("feed_ts"),
        )
        .withWatermark("position_ts", "30 minutes")
        .dropDuplicatesWithinWatermark(["vehicle_id", "position_ts"])
    )


# ---------------------------------------------------------------- the join

@dp.materialized_view(
    name="silver_arrivals_enriched",
    comment="Realized arrivals joined to schedule, trip, line and stop. Basis for all gold tables.",
    table_properties={"quality": "silver"},
    cluster_by=["service_date", "line"],
)
@dp.expect("schedule_matched", "scheduled_epoch IS NOT NULL")
@dp.expect("line_matched", "route_short_name IS NOT NULL")
@dp.expect_or_drop("delay_known", "delay_s IS NOT NULL")
def silver_arrivals_enriched():
    return enrich_arrivals(
        spark.read.table("silver_stop_arrivals"),  # noqa: F821
        spark.read.table("silver_stop_times"),  # noqa: F821
        spark.read.table("silver_trips"),  # noqa: F821
        spark.read.table("silver_routes"),  # noqa: F821
        spark.read.table("silver_stops"),  # noqa: F821
    )
