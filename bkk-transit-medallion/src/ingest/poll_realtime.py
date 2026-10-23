# Databricks notebook source
# MAGIC %md
# MAGIC # Poll BKK GTFS-Realtime → landing volume
# MAGIC
# MAGIC Fetches `TripUpdates.pb` and `VehiclePositions.pb` every `poll_seconds` for
# MAGIC `run_minutes`, decodes each snapshot to NDJSON and writes one file per snapshot:
# MAGIC
# MAGIC ```
# MAGIC /Volumes/<catalog>/<schema>/landing/rt/<feed>/date=YYYY-MM-DD/<feed_epoch>.json
# MAGIC /Volumes/<catalog>/<schema>/landing/rt_raw/<feed>/date=YYYY-MM-DD/<feed_epoch>.pb   (optional)
# MAGIC ```
# MAGIC
# MAGIC File name = the feed header timestamp, so an unchanged snapshot fetched twice is
# MAGIC written once (idempotent), and Auto Loader never sees duplicates from re-polls.

# COMMAND ----------

# MAGIC %pip install -q "gtfs-realtime-bindings>=1.0.0"

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "bkk_transit")
dbutils.widgets.text("secret_scope", "bkk")
dbutils.widgets.text("secret_key", "api_key")
dbutils.widgets.text("poll_seconds", "30")
dbutils.widgets.text("run_minutes", "55")
dbutils.widgets.dropdown("keep_raw_pb", "true", ["true", "false"])

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
poll_seconds = int(dbutils.widgets.get("poll_seconds"))
run_minutes = int(dbutils.widgets.get("run_minutes"))
keep_raw = dbutils.widgets.get("keep_raw_pb") == "true"
api_key = dbutils.secrets.get(dbutils.widgets.get("secret_scope"), dbutils.widgets.get("secret_key"))

LANDING = f"/Volumes/{catalog}/{schema}/landing"

# COMMAND ----------

import os
import time
from datetime import datetime, timezone

from bkk_rt import BASE_URL, DECODERS, FEEDS, fetch, feed_timestamp, to_ndjson


def write_snapshot(feed_name: str, payload: bytes) -> str | None:
    ts = feed_timestamp(payload)
    day = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    json_dir = f"{LANDING}/rt/{feed_name}/date={day}"
    json_path = f"{json_dir}/{ts}.json"
    if os.path.exists(json_path):
        return None  # same snapshot as last poll
    os.makedirs(json_dir, exist_ok=True)
    if keep_raw:
        raw_dir = f"{LANDING}/rt_raw/{feed_name}/date={day}"
        os.makedirs(raw_dir, exist_ok=True)
        with open(f"{raw_dir}/{ts}.pb", "wb") as fh:
            fh.write(payload)
    # write-then-rename so Auto Loader never picks up a half-written file
    tmp = f"{json_dir}/_tmp_{ts}.json.partial"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(to_ndjson(DECODERS[feed_name](payload)))
    os.replace(tmp, json_path)
    return json_path


deadline = time.monotonic() + run_minutes * 60
stats = {name: {"written": 0, "skipped": 0, "errors": 0} for name in FEEDS}

while time.monotonic() < deadline:
    tick = time.monotonic()
    for name, file in FEEDS.items():
        try:
            path = write_snapshot(name, fetch(f"{BASE_URL}/{file}", key=api_key))
            stats[name]["written" if path else "skipped"] += 1
        except Exception as err:  # keep polling; one bad response shouldn't kill the hour
            stats[name]["errors"] += 1
            print(f"[{datetime.now(timezone.utc):%H:%M:%S}] {name}: {err}")
    time.sleep(max(0.0, poll_seconds - (time.monotonic() - tick)))

print(stats)
if all(s["written"] == 0 for s in stats.values()):
    raise RuntimeError(f"No realtime snapshots written this run: {stats}")
