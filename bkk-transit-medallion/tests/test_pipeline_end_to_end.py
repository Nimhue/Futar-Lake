"""Runs the real pipeline files bronze -> silver -> gold on the synthetic landing zone."""
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from local_runtime import LocalPipeline
from make_sample_landing import CANCELED_TRIP, SKIPPED, generate

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def result(spark, tmp_path_factory):
    landing = tmp_path_factory.mktemp("landing")
    truth = generate(landing)
    pipe = LocalPipeline(spark, ROOT / "src" / "pipeline", {"bkk.landing_path": str(landing)})
    return pipe, pipe.run(), spark.createDataFrame(truth)


def test_graph_has_all_layers(result):
    _, tables, _ = result
    expected = {
        "bronze_static_routes", "bronze_static_trips", "bronze_static_stops", "bronze_static_stop_times",
        "bronze_rt_trip_updates", "bronze_rt_vehicle_positions",
        "silver_routes", "silver_trips", "silver_stops", "silver_stop_times",
        "silver_rt_stop_updates", "silver_stop_arrivals", "silver_vehicle_positions", "silver_arrivals_enriched",
        "gold_delay_by_line_hour", "gold_line_daily_reliability", "gold_stop_delay_hotspots",
        "gold_delay_propagation", "gold_feed_health", "gold_vehicle_latest",
    }
    assert expected <= set(tables)


def test_bom_stripped_and_latest_snapshot_wins(result):
    _, t, _ = result
    assert "route_id" in t["bronze_static_routes"].columns
    assert t["bronze_static_routes"].select("snapshot_id").distinct().count() == 2
    names = {r.route_long_name for r in t["silver_routes"].collect()}
    assert not any("(old)" in n for n in names), "SCD1 must keep the newest snapshot's values"
    assert t["silver_routes"].count() == 4


def test_one_row_per_stop_visit(result):
    _, t, _ = result
    a = t["silver_stop_arrivals"]
    assert a.count() == a.select("service_date", "trip_id", "stop_sequence").distinct().count()
    # many predictions were collapsed
    assert t["silver_rt_stop_updates"].count() > 5 * a.count()


def test_canceled_and_skipped_are_excluded(result):
    _, t, _ = result
    a = t["silver_stop_arrivals"]
    assert a.where(F.col("trip_id") == CANCELED_TRIP).count() == 0
    assert a.where((F.col("trip_id") == SKIPPED[0]) & (F.col("stop_sequence") == SKIPPED[1])).count() == 0


def test_delays_match_ground_truth_exactly(result):
    """Both delay paths (feed field for tram/metro, computed from schedule for bus/trolley,
    including the DST day and the after-midnight trips) must recover the true delay."""
    _, t, truth = result
    e = t["silver_arrivals_enriched"]
    joined = e.join(truth.select("service_date", "trip_id", "stop_sequence", F.col("delay_s").alias("true_delay")),
                    ["service_date", "trip_id", "stop_sequence"])
    assert joined.count() == truth.count() == e.count()
    assert joined.where("delay_s <> true_delay").count() == 0
    sources = {r.delay_source for r in e.select("delay_source").distinct().collect()}
    assert sources == {"feed", "computed"}
    # computed path specifically on the DST day
    dst = joined.where("service_date = DATE'2026-10-25' AND delay_source = 'computed'")
    assert dst.count() > 0 and dst.where("delay_s <> true_delay").count() == 0


def test_gold_by_line_hour_matches_truth(result):
    _, t, truth = result
    g = t["gold_delay_by_line_hour"].groupBy("line").agg(
        (F.sum(F.col("avg_delay_s") * F.col("observations")) / F.sum("observations")).alias("avg"))
    want = truth.groupBy("line").agg(F.avg("delay_s").alias("avg"))
    for row in g.join(want.withColumnRenamed("avg", "want"), "line").collect():
        assert abs(row.avg - row.want) < 0.2, row  # per-group averages are rounded to 0.1 s
    # rush hour is slower on the bus line
    hours = {r.hour_local: r.avg_delay_s for r in t["gold_delay_by_line_hour"].where("line = '7' AND day_type='weekday'").collect()}
    assert hours[8] > hours[6]
    # the after-midnight trips are bucketed at local hour 0
    assert t["gold_delay_by_line_hour"].where("hour_local = 0").count() > 0


def test_shares_are_valid_fractions(result):
    _, t, _ = result
    g = t["gold_line_daily_reliability"]
    assert g.where("on_time_share < 0 OR on_time_share > 1 OR late_over_5min_share > 1").count() == 0
    assert {r.mode for r in g.select("mode").distinct().collect()} == {"tram", "metro", "bus", "trolleybus"}


def test_feed_health_and_vehicles(result):
    _, t, _ = result
    fh = t["gold_feed_health"]
    assert fh.where("off_contract_rows > 0").count() == 0
    assert {r.feed for r in fh.select("feed").distinct().collect()} == {"trip_updates", "vehicle_positions"}
    v = t["gold_vehicle_latest"]
    assert v.count() == v.select("vehicle_id").distinct().count() > 0
    assert v.where("line IS NULL").count() == 0


def test_no_rows_dropped_by_expectations_on_clean_data(result):
    pipe, _, _ = result
    q = pipe.registry.quality
    assert q["silver_rt_stop_updates"]["trip_id_present"] == 0
    assert q["silver_arrivals_enriched"]["schedule_matched"] == 0
    assert q["bronze_rt_trip_updates"]["on_contract"] == 0
