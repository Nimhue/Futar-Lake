import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
for p in (ROOT / "src" / "pipeline", ROOT / "src" / "ingest", ROOT / "scripts", ROOT / "tests"):
    sys.path.insert(0, str(p))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .appName("bkk-transit-tests")
        .config("spark.sql.session.timeZone", "UTC")  # same as the pipeline configuration
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
