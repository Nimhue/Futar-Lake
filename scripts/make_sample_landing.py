"""Generate a synthetic, BKK-shaped landing zone for local runs and tests.

Writes the exact layout the ingest notebooks produce:

  <out>/static/<table>/snapshot=YYYYMMDDHHMM/<table>.txt
  <out>/rt/<feed>/date=YYYY-MM-DD/<feed_epoch>.json

Realtime files are built as real GTFS-RT protobuf messages and decoded with the
production decoder (src/ingest/bkk_rt.py), so the JSON contract is exercised too.

Lines and stop names are Budapest-flavoured but the timetable and delays are
synthetic. Every realized arrival's true delay is returned so tests can check
that the pipeline recovers it exactly.

    python scripts/make_sample_landing.py ./sample_landing
"""
from __future__ import annotations

import csv
import random
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "ingest"))

from google.transit import gtfs_realtime_pb2 as rt  # noqa: E402

from bkk_rt import decode_trip_updates, decode_vehicle_positions, to_ndjson  # noqa: E402

TZ = ZoneInfo("Europe/Budapest")

# route_id, short name, type, color, stops (name, lat, lon), base delay s, includes feed delay field
LINES = [
    ("3060", "4", 0, "FFD800", [
        ("Széll Kálmán tér", 47.5076, 19.0254), ("Margit híd, budai hídfő", 47.5121, 19.0377),
        ("Jászai Mari tér", 47.5130, 19.0476), ("Nyugati pályaudvar", 47.5108, 19.0566),
        ("Oktogon", 47.5056, 19.0636), ("Király utca", 47.5016, 19.0678),
        ("Wesselényi utca", 47.4990, 19.0703), ("Blaha Lujza tér", 47.4965, 19.0707),
    ], 40, True),
    ("5200", "M2", 1, "E41F18", [
        ("Déli pályaudvar", 47.5004, 19.0244), ("Széll Kálmán tér", 47.5076, 19.0254),
        ("Batthyány tér", 47.5065, 19.0383), ("Kossuth Lajos tér", 47.5075, 19.0459),
        ("Deák Ferenc tér", 47.4977, 19.0544), ("Astoria", 47.4936, 19.0605),
        ("Blaha Lujza tér", 47.4965, 19.0707), ("Keleti pályaudvar", 47.5003, 19.0839),
    ], 10, True),
    ("0070", "7", 3, "009FE3", [
        ("Bosnyák tér", 47.5164, 19.1159), ("Keleti pályaudvar", 47.5003, 19.0839),
        ("Blaha Lujza tér", 47.4965, 19.0707), ("Astoria", 47.4936, 19.0605),
        ("Ferenciek tere", 47.4935, 19.0558), ("Szent Gellért tér", 47.4840, 19.0527),
        ("Móricz Zsigmond körtér", 47.4778, 19.0466), ("Kelenföld vasútállomás", 47.4643, 19.0204),
    ], 120, False),
    ("4720", "72", 11, "FF1609", [
        ("Arany János utca", 47.5040, 19.0530), ("Bajcsy-Zsilinszky út", 47.5070, 19.0560),
        ("Podmaniczky utca", 47.5120, 19.0630), ("Szondi utca", 47.5150, 19.0680),
        ("Rippl-Rónai utca", 47.5185, 19.0745), ("Városliget", 47.5205, 19.0820),
        ("Hermina út", 47.5230, 19.0890), ("Zugló vasútállomás", 47.5100, 19.1050),
    ], 70, False),
]

HEADWAY_MIN = 10
FIRST_DEP_MIN, LAST_DEP_MIN = 6 * 60, 9 * 60 + 30
STOP_GAP_S = 150
POLL_S = 120
SERVICE_DATES = [date(2026, 10, 21), date(2026, 10, 25)]  # a Wednesday, and the DST-change Sunday
CANCELED_TRIP = "0070_2026_0700"
SKIPPED = ("4720_2026_0800", 4)


def hms(sec: int) -> str:
    return f"{sec // 3600:02d}:{sec % 3600 // 60:02d}:{sec % 60:02d}"


def service_origin(d: date) -> int:
    """'Noon minus 12h' of the service date, as epoch seconds."""
    noon = datetime(d.year, d.month, d.day, 12, tzinfo=TZ)
    return int(noon.timestamp()) - 12 * 3600


def build_timetable():
    trips, stop_times = [], []
    for route_id, _, _, _, stops, _, _ in LINES:
        dep = FIRST_DEP_MIN
        while dep <= LAST_DEP_MIN:
            trip_id = f"{route_id}_2026_{dep // 60:02d}{dep % 60:02d}"
            trips.append((trip_id, route_id))
            for i in range(len(stops)):
                t = dep * 60 + i * STOP_GAP_S
                stop_times.append((trip_id, i + 1, f"{route_id}_S{i + 1}", t))
            dep += HEADWAY_MIN
        # a night trip that runs past midnight (GTFS hour >= 24)
        trip_id = f"{route_id}_2026_night"
        trips.append((trip_id, route_id))
        for i in range(len(stops)):
            stop_times.append((trip_id, i + 1, f"{route_id}_S{i + 1}", 24 * 3600 + 20 * 60 + i * STOP_GAP_S))
    return trips, stop_times


def write_static(out: Path, snapshot: str, long_name_suffix: str = "", bom: bool = False):
    trips, stop_times = build_timetable()
    tables = {
        "routes": (["route_id", "agency_id", "route_short_name", "route_long_name", "route_type", "route_color"],
                   [(r, "BKK", s, f"Line {s}{long_name_suffix}", t, c) for r, s, t, c, *_ in LINES]),
        "stops": (["stop_id", "stop_name", "stop_lat", "stop_lon", "location_type"],
                  [(f"{r}_S{i + 1}", n, lat, lon, 0) for r, _, _, _, st, _, _ in LINES for i, (n, lat, lon) in enumerate(st)]),
        "trips": (["route_id", "trip_id", "service_id", "trip_headsign", "direction_id"],
                  [(r, t, "DAILY", "→", 0) for t, r in trips]),
        "stop_times": (["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"],
                       [(t, hms(s), hms(s), sid, seq) for t, seq, sid, s in stop_times]),
    }
    for name, (header, rows) in tables.items():
        d = out / "static" / name / f"snapshot={snapshot}"
        d.mkdir(parents=True, exist_ok=True)
        with open(d / f"{name}.txt", "w", newline="", encoding="utf-8-sig" if bom else "utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(header)
            w.writerows(rows)
    return trips, stop_times


def true_delay(rng_seed: str, base: int, seq: int, hour: int) -> int:
    rng = random.Random(rng_seed)
    rush = 1.8 if hour in (7, 8) else 1.0
    return int(base * rush * (0.4 + 0.15 * seq) + rng.randint(-45, 60))


def generate(out: Path) -> list[dict]:
    out = Path(out)
    write_static(out, "202610190400", long_name_suffix=" (old)", bom=True)
    trips, stop_times = write_static(out, "202610200400")  # newer snapshot must win in silver
    line_by_route = {r: (s, base, with_delay, stops) for r, s, _, _, stops, base, with_delay in LINES}
    by_trip: dict[str, list] = {}
    for t, seq, sid, s in stop_times:
        by_trip.setdefault(t, []).append((seq, sid, s))

    route_of = dict(trips)
    truth = []
    for sd in SERVICE_DATES:
        origin = service_origin(sd)
        # actual instant for every (trip, stop)
        actual = {}
        for trip_id, route_id in trips:
            if trip_id == CANCELED_TRIP:
                continue
            _, base, _, _ = line_by_route[route_id]
            for seq, sid, s in by_trip[trip_id]:
                sched = origin + s
                hour = datetime.fromtimestamp(sched, TZ).hour
                d = true_delay(f"{sd}{trip_id}{seq}", base, seq, hour)
                actual[(trip_id, seq)] = (sched, sched + d, d, sid)

        start = origin + FIRST_DEP_MIN * 60 - 600
        end = origin + 25 * 3600
        ts = start
        while ts <= end:
            tu_feed, vp_feed = rt.FeedMessage(), rt.FeedMessage()
            for f in (tu_feed, vp_feed):
                f.header.gtfs_realtime_version = "2.0"
                f.header.timestamp = ts
            for trip_id, route_id in trips:
                stops = by_trip[trip_id]
                first_sched = origin + stops[0][2]
                last_sched = origin + stops[-1][2]
                if not (first_sched - 900 <= ts <= last_sched + 900):
                    continue
                _, _, with_delay, coords = line_by_route[route_id]
                ent = tu_feed.entity.add(id=f"tu-{trip_id}")
                tu = ent.trip_update
                tu.trip.trip_id, tu.trip.route_id = trip_id, route_id
                tu.trip.start_date = sd.strftime("%Y%m%d")
                tu.vehicle.id = f"BKK_{route_id}_{trip_id[-4:]}"
                if trip_id == CANCELED_TRIP:
                    tu.trip.schedule_relationship = rt.TripDescriptor.CANCELED
                    continue
                last_passed = None
                for seq, sid, _ in stops:
                    sched, act, d, _ = actual[(trip_id, seq)]
                    stu = tu.stop_time_update.add(stop_sequence=seq, stop_id=sid)
                    if (trip_id, seq) == SKIPPED:
                        stu.schedule_relationship = rt.TripUpdate.StopTimeUpdate.SKIPPED
                        continue
                    if act <= ts:  # realized: report the actual
                        t_rep, d_rep, last_passed = act, d, seq
                    else:  # prediction: noisy, converging as it gets closer
                        err = int((act - ts) * 0.05) * (1 if seq % 2 else -1)
                        t_rep, d_rep = act + err, d + err
                    stu.arrival.time = t_rep
                    if with_delay:
                        stu.arrival.delay = d_rep
                # vehicle position near the last passed stop
                if last_passed:
                    _, lat, lon = coords[last_passed - 1]
                    v = vp_feed.entity.add(id=f"vp-{trip_id}").vehicle
                    v.vehicle.id = tu.vehicle.id
                    v.trip.trip_id, v.trip.route_id = trip_id, route_id
                    v.trip.start_date = tu.trip.start_date
                    v.position.latitude, v.position.longitude = lat, lon
                    v.current_stop_sequence = last_passed
                    v.timestamp = ts
            day = datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")
            for feed, msg, dec in (("trip_updates", tu_feed, decode_trip_updates),
                                   ("vehicle_positions", vp_feed, decode_vehicle_positions)):
                if not msg.entity:
                    continue
                d = out / "rt" / feed / f"date={day}"
                d.mkdir(parents=True, exist_ok=True)
                (d / f"{ts}.json").write_text(to_ndjson(dec(msg.SerializeToString())), encoding="utf-8")
            ts += POLL_S
            # skip the dead hours between the morning block and the night trips
            if origin + 10 * 3600 + 1800 < ts < origin + 24 * 3600:
                ts = origin + 24 * 3600

        # Only arrivals the poller actually saw as realized count as ground truth
        for (trip_id, seq), (sched, act, d, sid) in actual.items():
            if (trip_id, seq) == SKIPPED:
                continue
            truth.append({"service_date": sd, "trip_id": trip_id, "stop_sequence": seq,
                          "scheduled_epoch": sched, "actual_epoch": act, "delay_s": d,
                          "line": line_by_route[route_of[trip_id]][0]})
    return truth


if __name__ == "__main__":
    target = Path(sys.argv[1] if len(sys.argv) > 1 else "sample_landing")
    rows = generate(target)
    print(f"wrote sample landing zone to {target.resolve()} ({len(rows)} ground-truth arrivals)")
