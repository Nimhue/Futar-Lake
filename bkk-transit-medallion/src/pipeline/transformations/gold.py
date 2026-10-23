"""GOLD — business-ready reliability metrics, all materialized views.

'On time' window defaults to [-60 s, +180 s], a common transit-industry choice;
override with pipeline conf bkk.on_time_early_s / bkk.on_time_late_s.
"""
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

from transit_lib.transforms import delay_stats

EARLY_S = int(spark.conf.get("bkk.on_time_early_s", "60"))  # noqa: F821
LATE_S = int(spark.conf.get("bkk.on_time_late_s", "180"))  # noqa: F821
MIN_OBS_HOTSPOT = int(spark.conf.get("bkk.hotspot_min_observations", "30"))  # noqa: F821


def _arrivals():
    return spark.read.table("silver_arrivals_enriched")  # noqa: F821


@dp.materialized_view(
    name="gold_delay_by_line_hour",
    comment="Average / median / p90 delay and on-time share by line, weekday-vs-weekend and local hour.",
    table_properties={"quality": "gold"},
)
def gold_delay_by_line_hour():
    return delay_stats(_arrivals(), ["mode", "line", "route_id", "day_type", "hour_local"], EARLY_S, LATE_S)


@dp.materialized_view(
    name="gold_line_daily_reliability",
    comment="Daily reliability scorecard per line.",
    table_properties={"quality": "gold"},
)
def gold_line_daily_reliability():
    return delay_stats(_arrivals(), ["service_date", "mode", "line", "route_id"], EARLY_S, LATE_S)


@dp.materialized_view(
    name="gold_stop_delay_hotspots",
    comment="Stops where vehicles arrive latest on average (min observation threshold applied).",
    table_properties={"quality": "gold"},
)
def gold_stop_delay_hotspots():
    stats = delay_stats(_arrivals(), ["stop_id", "stop_name", "stop_lat", "stop_lon", "mode"], EARLY_S, LATE_S)
    return stats.where(F.col("observations") >= MIN_OBS_HOTSPOT)


@dp.materialized_view(
    name="gold_delay_propagation",
    comment="How delay builds along a line: avg delay by stop sequence bucket, per line and direction.",
    table_properties={"quality": "gold"},
)
def gold_delay_propagation():
    return (
        _arrivals()
        .withColumn("stop_sequence_bucket", (F.col("stop_sequence") / 5).cast("int") * 5)
        .groupBy("mode", "line", "direction_id", "stop_sequence_bucket")
        .agg(F.count("*").alias("observations"), F.round(F.avg("delay_s"), 1).alias("avg_delay_s"))
    )


@dp.materialized_view(
    name="gold_feed_health",
    comment="Ingestion observability: snapshots and entities per feed per hour, plus lag to landing.",
    table_properties={"quality": "gold"},
)
def gold_feed_health():
    def per_feed(table, feed):
        return (
            spark.read.table(table)  # noqa: F821
            .withColumn("feed_ts", F.timestamp_seconds("feed_timestamp"))
            .groupBy(F.lit(feed).alias("feed"), F.date_trunc("hour", "feed_ts").alias("hour_utc"))
            .agg(
                F.countDistinct("feed_timestamp").alias("snapshots"),
                F.count("*").alias("entities"),
                F.max("feed_ts").alias("latest_feed_ts"),
                F.round(F.avg(F.unix_timestamp("_file_modified_at") - F.col("feed_timestamp")), 1)
                .alias("avg_landing_lag_s"),
                F.sum(F.col("_rescued_data").isNotNull().cast("int")).alias("off_contract_rows"),
            )
        )

    return per_feed("bronze_rt_trip_updates", "trip_updates").unionByName(
        per_feed("bronze_rt_vehicle_positions", "vehicle_positions")
    )


@dp.materialized_view(
    name="gold_vehicle_latest",
    comment="Most recent known position of every vehicle, with line attributes (live map source).",
    table_properties={"quality": "gold"},
)
def gold_vehicle_latest():
    w = Window.partitionBy("vehicle_id").orderBy(F.col("position_ts").desc())
    routes = spark.read.table("silver_routes").select(  # noqa: F821
        "route_id", F.col("route_short_name").alias("line"), "route_type", "route_color")
    return (
        spark.read.table("silver_vehicle_positions")  # noqa: F821
        .withColumn("rn", F.row_number().over(w))
        .where("rn = 1")
        .drop("rn")
        .join(routes, "route_id", "left")
    )
