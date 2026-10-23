# Databricks notebook source
# MAGIC %md
# MAGIC # Download BKK static GTFS → landing volume
# MAGIC
# MAGIC Downloads `budapest_gtfs.zip` (no API key needed), and unpacks the tables the
# MAGIC pipeline uses into one folder per table, partitioned by snapshot date:
# MAGIC
# MAGIC ```
# MAGIC /Volumes/<catalog>/<schema>/landing/static/<table>/snapshot=YYYYMMDDHHMM/<table>.txt
# MAGIC ```
# MAGIC
# MAGIC BKK refreshes the feed every few days. If the zip is byte-identical to the last
# MAGIC one we landed (SHA-256 match), nothing is written, so daily scheduling is cheap.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "bkk_transit")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
LANDING = f"/Volumes/{catalog}/{schema}/landing"

# COMMAND ----------

import hashlib
import io
import json
import os
import zipfile
from datetime import datetime, timezone

from bkk_rt import STATIC_GTFS_URL, fetch

TABLES = ["agency", "routes", "trips", "stops", "stop_times", "calendar_dates", "feed_info"]
MANIFEST = f"{LANDING}/static/_manifest.json"

payload = fetch(STATIC_GTFS_URL, timeout=300)
digest = hashlib.sha256(payload).hexdigest()

manifest = []
if os.path.exists(MANIFEST):
    with open(MANIFEST) as fh:
        manifest = json.load(fh)

if manifest and manifest[-1]["sha256"] == digest:
    dbutils.notebook.exit(f"unchanged ({digest[:12]}), nothing landed")

# minute precision: two different feeds on the same day get separate folders
snapshot = datetime.now(timezone.utc).strftime("%Y%m%d%H%M")
landed = {}
with zipfile.ZipFile(io.BytesIO(payload)) as zf:
    names = {os.path.splitext(os.path.basename(n))[0]: n for n in zf.namelist() if n.endswith(".txt")}
    for table in TABLES:
        if table not in names:
            continue
        out_dir = f"{LANDING}/static/{table}/snapshot={snapshot}"
        os.makedirs(out_dir, exist_ok=True)
        tmp, final = f"{out_dir}/_{table}.txt.partial", f"{out_dir}/{table}.txt"
        with zf.open(names[table]) as src, open(tmp, "wb") as dst:
            while chunk := src.read(8 << 20):
                dst.write(chunk)
        os.replace(tmp, final)
        landed[table] = os.path.getsize(final)

missing = {"routes", "trips", "stops", "stop_times"} - landed.keys()
if missing:
    raise RuntimeError(f"Static GTFS is missing required tables: {sorted(missing)}")

manifest.append({"snapshot": snapshot, "sha256": digest, "bytes": len(payload), "tables": landed,
                 "landed_at": datetime.now(timezone.utc).isoformat()})
os.makedirs(os.path.dirname(MANIFEST), exist_ok=True)
with open(MANIFEST, "w") as fh:
    json.dump(manifest, fh, indent=2)

print(json.dumps(manifest[-1], indent=2))
