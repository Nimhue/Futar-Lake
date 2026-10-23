"""Run the whole pipeline on synthetic data on your laptop and print the gold tables.

    pip install -r requirements-dev.txt
    python scripts/run_local.py
"""
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for p in ("src/pipeline", "src/ingest", "scripts", "tests"):
    sys.path.insert(0, str(ROOT / p))
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession  # noqa: E402

from local_runtime import LocalPipeline  # noqa: E402
from make_sample_landing import generate  # noqa: E402

spark = (SparkSession.builder.master("local[2]").config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "4").config("spark.ui.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

with tempfile.TemporaryDirectory() as landing:
    generate(Path(landing))
    tables = LocalPipeline(spark, ROOT / "src" / "pipeline", {"bkk.landing_path": landing}).run()

    print("\n== row counts ==")
    for name in sorted(tables):
        print(f"  {name:<32} {tables[name].count():>7}")

    print("\n== gold_delay_by_line_hour (weekday, line 7) ==")
    (tables["gold_delay_by_line_hour"].where("day_type = 'weekday' AND line = '7'")
     .orderBy("hour_local").drop("route_id").show(truncate=False))

    print("== gold_stop_delay_hotspots (top 5) ==")
    (tables["gold_stop_delay_hotspots"].orderBy("avg_delay_s", ascending=False)
     .select("stop_name", "mode", "observations", "avg_delay_s", "p90_delay_s", "on_time_share").show(5, truncate=False))

    print("== gold_line_daily_reliability ==")
    (tables["gold_line_daily_reliability"].orderBy("service_date", "line")
     .select("service_date", "mode", "line", "trips", "avg_delay_s", "p90_delay_s", "on_time_share", "late_over_5min_share")
     .show(truncate=False))
spark.stop()
