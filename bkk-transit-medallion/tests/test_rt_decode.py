import json

from google.transit import gtfs_realtime_pb2 as rt

from bkk_rt import decode_trip_updates, decode_vehicle_positions, to_ndjson
from transit_lib.schemas import TRIP_UPDATE, VEHICLE_POSITION


def _feed(ts=1_792_560_000):
    f = rt.FeedMessage()
    f.header.gtfs_realtime_version = "2.0"
    f.header.timestamp = ts
    return f


def test_trip_update_optional_fields_become_null():
    f = _feed()
    tu = f.entity.add(id="e1").trip_update
    tu.trip.trip_id, tu.trip.route_id, tu.trip.start_date = "T1", "3060", "20261021"
    s1 = tu.stop_time_update.add(stop_sequence=1, stop_id="S1")
    s1.arrival.time, s1.arrival.delay = 1_792_559_000, 75
    s2 = tu.stop_time_update.add(stop_sequence=2, stop_id="S2")
    s2.departure.time = 1_792_559_200  # no arrival, no delay
    f.entity.add(id="alert-only").alert.header_text.translation.add(text="x")  # ignored

    rows = decode_trip_updates(f.SerializeToString())
    assert len(rows) == 1
    r = rows[0]
    assert r["feed_timestamp"] == 1_792_560_000 and r["trip_id"] == "T1" and r["vehicle_id"] is None
    assert r["stop_time_updates"][0] == {
        "stop_sequence": 1, "stop_id": "S1", "arrival_time": 1_792_559_000, "arrival_delay": 75,
        "departure_time": None, "departure_delay": None, "schedule_relationship": None,
    }
    assert r["stop_time_updates"][1]["arrival_time"] is None
    assert r["stop_time_updates"][1]["departure_time"] == 1_792_559_200


def test_vehicle_position_decode():
    f = _feed()
    v = f.entity.add(id="v1").vehicle
    v.vehicle.id, v.vehicle.license_plate = "BKK_V1", "ABC-123"
    v.trip.trip_id = "T1"
    v.position.latitude, v.position.longitude = 47.4979, 19.0402
    v.current_status = rt.VehiclePosition.STOPPED_AT
    (r,) = decode_vehicle_positions(f.SerializeToString())
    assert r["vehicle_id"] == "BKK_V1" and r["license_plate"] == "ABC-123"
    assert abs(r["latitude"] - 47.4979) < 1e-4 and r["current_status"] == 1 and r["speed"] is None


def test_json_contract_matches_bronze_schema(spark, tmp_path):
    """The decoder's JSON must read cleanly with the pipeline's declared schema (no rescued data)."""
    f = _feed()
    tu = f.entity.add(id="e1").trip_update
    tu.trip.trip_id, tu.trip.start_date = "T1", "20261021"
    tu.stop_time_update.add(stop_sequence=1).arrival.time = 1
    v = f.entity.add(id="v1").vehicle
    v.vehicle.id = "V1"
    v.position.latitude, v.position.longitude = 47.5, 19.0
    payload = f.SerializeToString()

    for name, decoder, schema in (("tu", decode_trip_updates, TRIP_UPDATE), ("vp", decode_vehicle_positions, VEHICLE_POSITION)):
        path = tmp_path / f"{name}.json"
        path.write_text(to_ndjson(decoder(payload)))
        # every key the decoder emits must be declared in the schema
        assert set(json.loads(path.read_text().splitlines()[0])) == set(schema.fieldNames())
        # rescuedDataColumn is Databricks-only; FAILFAST is the open-source equivalent check
        df = spark.read.schema(schema).option("mode", "FAILFAST").json(str(path))
        assert df.count() == 1 and df.collect()[0]["feed_timestamp"] == 1_792_560_000


def test_empty_feed_writes_empty_file():
    assert to_ndjson(decode_trip_updates(_feed().SerializeToString())) == ""
