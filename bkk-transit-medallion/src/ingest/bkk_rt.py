"""BKK FUTÁR GTFS-Realtime client and protobuf -> NDJSON decoder.

Kept free of Databricks imports so it can be unit-tested locally. The decoder
defines the bronze JSON contract mirrored by src/pipeline/transit_lib/schemas.py.

Endpoints (API key from https://opendata.bkk.hu, passed as ?key=):
  https://go.bkk.hu/api/query/v1/ws/gtfs-rt/full/TripUpdates.pb
  https://go.bkk.hu/api/query/v1/ws/gtfs-rt/full/VehiclePositions.pb
"""
from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request

from google.transit import gtfs_realtime_pb2

BASE_URL = "https://go.bkk.hu/api/query/v1/ws/gtfs-rt/full"
STATIC_GTFS_URL = "https://go.bkk.hu/api/static/v1/public-gtfs/budapest_gtfs.zip"
FEEDS = {"trip_updates": "TripUpdates.pb", "vehicle_positions": "VehiclePositions.pb"}


def fetch(url: str, key: str | None = None, timeout: int = 30, retries: int = 3) -> bytes:
    if key:
        url = f"{url}?{urllib.parse.urlencode({'key': key})}"
    last_err = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "bkk-transit-medallion/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except Exception as err:  # noqa: BLE001 - retry any transport error
            last_err = err
            time.sleep(2 ** attempt)
    raise RuntimeError(f"GET {url.split('?')[0]} failed after {retries} attempts") from last_err


def _opt(msg, field, default=None):
    """Protobuf optional field -> value or None (proto2 HasField semantics)."""
    return getattr(msg, field) if msg.HasField(field) else default


def _trip_fields(trip) -> dict:
    return {
        "trip_id": _opt(trip, "trip_id"),
        "route_id": _opt(trip, "route_id"),
        "direction_id": _opt(trip, "direction_id"),
        "start_date": _opt(trip, "start_date"),
        "start_time": _opt(trip, "start_time"),
        "trip_schedule_relationship": _opt(trip, "schedule_relationship"),
    }


def parse_feed(payload: bytes) -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(payload)
    return feed


def decode_trip_updates(payload: bytes) -> list[dict]:
    feed = parse_feed(payload)
    feed_ts = feed.header.timestamp
    rows = []
    for ent in feed.entity:
        if not ent.HasField("trip_update"):
            continue
        tu = ent.trip_update
        stus = []
        for s in tu.stop_time_update:
            arr = s.arrival if s.HasField("arrival") else None
            dep = s.departure if s.HasField("departure") else None
            stus.append({
                "stop_sequence": _opt(s, "stop_sequence"),
                "stop_id": _opt(s, "stop_id"),
                "arrival_time": _opt(arr, "time") if arr else None,
                "arrival_delay": _opt(arr, "delay") if arr else None,
                "departure_time": _opt(dep, "time") if dep else None,
                "departure_delay": _opt(dep, "delay") if dep else None,
                "schedule_relationship": _opt(s, "schedule_relationship"),
            })
        veh = tu.vehicle if tu.HasField("vehicle") else None
        rows.append({
            "feed_timestamp": feed_ts,
            "entity_id": ent.id,
            **_trip_fields(tu.trip),
            "vehicle_id": _opt(veh, "id") if veh else None,
            "vehicle_label": _opt(veh, "label") if veh else None,
            "timestamp": _opt(tu, "timestamp"),
            "delay": _opt(tu, "delay"),
            "stop_time_updates": stus,
        })
    return rows


def decode_vehicle_positions(payload: bytes) -> list[dict]:
    feed = parse_feed(payload)
    feed_ts = feed.header.timestamp
    rows = []
    for ent in feed.entity:
        if not ent.HasField("vehicle"):
            continue
        vp = ent.vehicle
        veh = vp.vehicle if vp.HasField("vehicle") else None
        pos = vp.position if vp.HasField("position") else None
        trip = _trip_fields(vp.trip) if vp.HasField("trip") else {}
        rows.append({
            "feed_timestamp": feed_ts,
            "entity_id": ent.id,
            "vehicle_id": _opt(veh, "id") if veh else None,
            "vehicle_label": _opt(veh, "label") if veh else None,
            "license_plate": _opt(veh, "license_plate") if veh else None,
            "trip_id": trip.get("trip_id"),
            "route_id": trip.get("route_id"),
            "direction_id": trip.get("direction_id"),
            "start_date": trip.get("start_date"),
            "latitude": pos.latitude if pos else None,
            "longitude": pos.longitude if pos else None,
            "bearing": _opt(pos, "bearing") if pos else None,
            "speed": _opt(pos, "speed") if pos else None,
            "current_stop_sequence": _opt(vp, "current_stop_sequence"),
            "stop_id": _opt(vp, "stop_id"),
            "current_status": _opt(vp, "current_status"),
            "timestamp": _opt(vp, "timestamp"),
        })
    return rows


DECODERS = {"trip_updates": decode_trip_updates, "vehicle_positions": decode_vehicle_positions}


def to_ndjson(rows: list[dict]) -> str:
    return "\n".join(json.dumps(r, separators=(",", ":"), ensure_ascii=False) for r in rows) + ("\n" if rows else "")


def feed_timestamp(payload: bytes) -> int:
    return parse_feed(payload).header.timestamp
