from datetime import date, datetime
from zoneinfo import ZoneInfo

from pyspark.sql import functions as F

from transit_lib.gtfs_time import gtfs_time_to_seconds, local_hour, scheduled_epoch

TZ = ZoneInfo("Europe/Budapest")


def test_times_past_midnight(spark):
    rows = spark.createDataFrame([("08:15:30",), ("25:13:00",), (" 7:05:00",), (None,), ("bad",)], ["t"])
    got = [r.s for r in rows.select(gtfs_time_to_seconds(F.col("t")).alias("s")).collect()]
    assert got == [8 * 3600 + 15 * 60 + 30, 25 * 3600 + 13 * 60, 7 * 3600 + 5 * 60, None, None]


def _sched(spark, d, hms):
    df = spark.createDataFrame([(d, hms)], ["d", "t"])
    return df.select(scheduled_epoch(F.col("d"), gtfs_time_to_seconds(F.col("t"))).alias("e")).first().e


def test_scheduled_epoch_normal_day(spark):
    expected = int(datetime(2026, 10, 21, 8, 0, tzinfo=TZ).timestamp())
    assert _sched(spark, date(2026, 10, 21), "08:00:00") == expected


def test_scheduled_epoch_dst_fall_back_day(spark):
    # 25 Oct 2026: clocks go 03:00 CEST -> 02:00 CET. GTFS counts from noon-minus-12h,
    # so 08:00:00 must land on 08:00 CET, not 07:00 (what midnight + 8h would give).
    expected = int(datetime(2026, 10, 25, 8, 0, tzinfo=TZ).timestamp())
    assert _sched(spark, date(2026, 10, 25), "08:00:00") == expected


def test_scheduled_epoch_after_midnight_trip(spark):
    expected = int(datetime(2026, 10, 22, 0, 20, tzinfo=TZ).timestamp())
    assert _sched(spark, date(2026, 10, 21), "24:20:00") == expected


def test_local_hour(spark):
    e = int(datetime(2026, 7, 1, 8, 30, tzinfo=TZ).timestamp())  # 06:30 UTC in summer
    df = spark.createDataFrame([(e,)], ["e"])
    assert df.select(local_hour(F.col("e")).alias("h")).first().h == 8
