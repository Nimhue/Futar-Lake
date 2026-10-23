"""GTFS time arithmetic as Spark Column expressions.

Two GTFS quirks make "delay" harder than it looks:

1. stop_times.arrival_time can exceed 24:00:00 ("25:13:00" = 01:13 the next
   calendar day, still on the previous *service day*). Night buses live here.
2. GTFS times are measured from "noon minus 12h" of the service date, not from
   midnight. On DST-change days (last Sunday of March / October in Budapest)
   midnight and noon-minus-12h differ by an hour, so a naive
   `date + seconds` is wrong for every trip that day.

All functions assume the session time zone is UTC (the pipeline sets
spark.sql.session.timeZone = UTC), so a Spark TIMESTAMP is a UTC instant.
"""
from pyspark.sql import Column
from pyspark.sql import functions as F

LOCAL_TZ = "Europe/Budapest"


def gtfs_time_to_seconds(col: Column) -> Column:
    """'HH:MM:SS' (HH may be >= 24) -> seconds since service-day origin. Null-safe."""
    parts = F.split(F.trim(col), ":")
    valid = col.isNotNull() & (F.size(parts) == 3)
    seconds = (
        parts.getItem(0).cast("int") * 3600
        + parts.getItem(1).cast("int") * 60
        + parts.getItem(2).cast("int")
    )
    return F.when(valid, seconds)


def service_day_origin_epoch(service_date: Column, tz: str = LOCAL_TZ) -> Column:
    """Epoch seconds of 'noon minus 12h' on service_date in the agency time zone."""
    local_noon = F.to_timestamp(
        F.concat(F.date_format(service_date, "yyyy-MM-dd"), F.lit(" 12:00:00"))
    )
    # to_utc_timestamp: interpret the wall-clock value as `tz`, return the UTC instant
    return F.unix_timestamp(F.to_utc_timestamp(local_noon, tz)) - F.lit(12 * 3600)


def scheduled_epoch(service_date: Column, gtfs_seconds: Column, tz: str = LOCAL_TZ) -> Column:
    """Scheduled instant (epoch seconds) of a stop_times entry on a given service date."""
    return service_day_origin_epoch(service_date, tz) + gtfs_seconds


def local_hour(epoch_seconds: Column, tz: str = LOCAL_TZ) -> Column:
    """Hour of day (0-23) in the agency time zone for an epoch-seconds column."""
    return F.hour(F.from_utc_timestamp(F.timestamp_seconds(epoch_seconds), tz))


def parse_service_date(yyyymmdd: Column) -> Column:
    """GTFS / GTFS-RT 'YYYYMMDD' string -> DATE."""
    return F.to_date(yyyymmdd, "yyyyMMdd")
