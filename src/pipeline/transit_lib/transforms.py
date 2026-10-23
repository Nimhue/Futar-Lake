"""DataFrame-level transforms for silver and gold. Pure functions: DataFrame in, DataFrame out."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from transit_lib.gtfs_time import gtfs_time_to_seconds, local_hour, parse_service_date, scheduled_epoch
from transit_lib.labels import route_type_label

# How far after the feed timestamp an event may be and still count as "happened".
# GTFS-RT arrival times for passed stops are actuals; future ones are predictions.
REALIZED_TOLERANCE_S = 30


def flatten_stop_time_updates(trip_updates: DataFrame) -> DataFrame:
    """One row per (feed snapshot, trip, stop) from the nested bronze trip updates."""
    exploded = trip_updates.select(
        "feed_timestamp", "trip_id", "route_id", "direction_id", "start_date",
        "trip_schedule_relationship", "vehicle_id",
        F.explode_outer("stop_time_updates").alias("stu"),
    )
    event_epoch = F.coalesce(F.col("stu.arrival_time"), F.col("stu.departure_time"))
    return (
        exploded
        .where(F.col("stu").isNotNull())
        .select(
            parse_service_date(F.col("start_date")).alias("service_date"),
            "trip_id", "route_id", "direction_id", "vehicle_id",
            "trip_schedule_relationship",
            F.col("stu.stop_sequence").alias("stop_sequence"),
            F.col("stu.stop_id").alias("stop_id"),
            F.col("stu.schedule_relationship").alias("stop_schedule_relationship"),
            event_epoch.alias("event_epoch"),
            F.coalesce(F.col("stu.arrival_delay"), F.col("stu.departure_delay")).alias("reported_delay_s"),
            F.col("feed_timestamp").alias("feed_epoch"),
            F.timestamp_seconds("feed_timestamp").alias("feed_ts"),
            (event_epoch <= F.col("feed_timestamp") + REALIZED_TOLERANCE_S).alias("is_realized"),
        )
    )


def type_stop_times(stop_times: DataFrame) -> DataFrame:
    return stop_times.select(
        "trip_id",
        F.col("stop_sequence").cast("int").alias("stop_sequence"),
        "stop_id",
        gtfs_time_to_seconds(F.col("arrival_time")).alias("sched_arrival_s"),
        gtfs_time_to_seconds(F.col("departure_time")).alias("sched_departure_s"),
        "snapshot_seq",
        "snapshot_date",
    )


def enrich_arrivals(arrivals: DataFrame, stop_times: DataFrame, trips: DataFrame,
                    routes: DataFrame, stops: DataFrame) -> DataFrame:
    """Realized arrivals + schedule + line/stop attributes -> analysis-ready fact table.

    Delay preference: the feed's own `delay` field when present, otherwise
    (actual instant - scheduled instant) computed from the static timetable.
    """
    st = stop_times.select("trip_id", "stop_sequence", "sched_arrival_s", "sched_departure_s")
    tr = trips.select("trip_id", F.col("route_id").alias("trip_route_id"), "trip_headsign")
    rt = routes.select("route_id", "route_short_name", "route_type", "route_color")
    sp = stops.select(F.col("stop_id").alias("s_stop_id"), "stop_name", "stop_lat", "stop_lon")

    sched_s = F.coalesce(F.col("sched_arrival_s"), F.col("sched_departure_s"))
    sched_epoch = scheduled_epoch(F.col("service_date"), sched_s)
    computed_delay = F.col("event_epoch") - sched_epoch

    return (
        arrivals.alias("a")
        .join(st, ["trip_id", "stop_sequence"], "left")
        .join(tr, "trip_id", "left")
        .withColumn("route_id", F.coalesce(F.col("a.route_id"), F.col("trip_route_id")))
        .drop("trip_route_id")
        .join(rt, "route_id", "left")
        .join(sp, F.col("a.stop_id") == F.col("s_stop_id"), "left")
        .drop("s_stop_id")
        .withColumn("scheduled_epoch", sched_epoch)
        .withColumn("delay_s", F.coalesce(F.col("reported_delay_s"), computed_delay))
        .withColumn("delay_source", F.when(F.col("reported_delay_s").isNotNull(), "feed")
                    .when(computed_delay.isNotNull(), "computed"))
        .withColumn("hour_local", local_hour(F.coalesce(sched_epoch, F.col("event_epoch"))))
        .withColumn("day_type", F.when(F.dayofweek("service_date").isin(1, 7), "weekend").otherwise("weekday"))
        .withColumn("line", F.coalesce(F.col("route_short_name"), F.col("route_id")))
        .withColumn("mode", route_type_label(F.col("route_type")))
    )


def delay_stats(df: DataFrame, group_cols: list, on_time_early_s: int = 60,
                on_time_late_s: int = 180) -> DataFrame:
    """Standard reliability aggregates. 'On time' = within [-early, +late] seconds."""
    d = F.col("delay_s")
    return df.groupBy(*group_cols).agg(
        F.count("*").alias("observations"),
        F.countDistinct("trip_id", "service_date").alias("trips"),
        F.round(F.avg(d), 1).alias("avg_delay_s"),
        F.percentile_approx(d, 0.5).alias("p50_delay_s"),
        F.percentile_approx(d, 0.9).alias("p90_delay_s"),
        F.round(F.avg(((d >= -on_time_early_s) & (d <= on_time_late_s)).cast("double")), 4).alias("on_time_share"),
        F.round(F.avg((d > 300).cast("double")), 4).alias("late_over_5min_share"),
        F.round(F.avg((d < -on_time_early_s).cast("double")), 4).alias("early_share"),
    )
