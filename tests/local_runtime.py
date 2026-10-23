"""A tiny local stand-in for the Lakeflow declarative pipelines runtime.

It lets the *real* files in src/pipeline/transformations run end to end on a laptop:

* a stub `pyspark.pipelines` module records every table, view, expectation and
  AUTO CDC flow the files declare;
* a thin Spark wrapper turns `readStream.format("cloudFiles")` into a batch file
  read (same options, same _metadata column) and `readStream.table` /
  `read.table` into lookups of already-computed datasets;
* AUTO CDC SCD1 is emulated as "latest row per key by sequence_by".

It is a test harness, not a re-implementation: no checkpoints, no incremental
processing. Its job is to prove the dataflow graph, joins and math are right.
"""
from __future__ import annotations

import importlib.util
import sys
import types
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


@dataclass
class Dataset:
    name: str
    kind: str  # table | materialized_view | view | streaming_table
    func: object = None
    expect: dict = field(default_factory=dict)
    expect_or_drop: dict = field(default_factory=dict)
    expect_or_fail: dict = field(default_factory=dict)
    cdc: dict | None = None


class Registry:
    def __init__(self):
        self.datasets: dict[str, Dataset] = {}
        self.quality: dict[str, dict] = {}

    # -- decorator plumbing -------------------------------------------------
    @staticmethod
    def _exp(func, attr, rules):
        bucket = getattr(func, attr, {})
        bucket.update(rules)
        setattr(func, attr, bucket)
        return func

    def _register(self, kind, name, func, **kw):
        ds = Dataset(
            name=name or func.__name__, kind=kind, func=func,
            expect={**getattr(func, "_expect", {}), **(kw.get("expect_all") or {})},
            expect_or_drop={**getattr(func, "_expect_or_drop", {}), **(kw.get("expect_all_or_drop") or {})},
            expect_or_fail={**getattr(func, "_expect_or_fail", {}), **(kw.get("expect_all_or_fail") or {})},
        )
        self.datasets[ds.name] = ds
        return func

    def module(self) -> types.ModuleType:
        reg = self
        m = types.ModuleType("pyspark.pipelines")

        def deco(kind):
            def outer(func=None, *, name=None, **kw):
                if callable(func):
                    return reg._register(kind, None, func)
                return lambda f: reg._register(kind, name, f, **kw)
            return outer

        m.table = deco("table")
        m.materialized_view = deco("materialized_view")
        m.temporary_view = deco("view")
        m.view = deco("view")
        m.expect = lambda n, c: (lambda f: reg._exp(f, "_expect", {n: c}))
        m.expect_or_drop = lambda n, c: (lambda f: reg._exp(f, "_expect_or_drop", {n: c}))
        m.expect_or_fail = lambda n, c: (lambda f: reg._exp(f, "_expect_or_fail", {n: c}))
        m.expect_all = lambda d: (lambda f: reg._exp(f, "_expect", d))
        m.expect_all_or_drop = lambda d: (lambda f: reg._exp(f, "_expect_or_drop", d))
        m.expect_all_or_fail = lambda d: (lambda f: reg._exp(f, "_expect_or_fail", d))

        def create_streaming_table(name, **kw):
            reg.datasets[name] = Dataset(
                name=name, kind="streaming_table",
                expect=kw.get("expect_all") or {}, expect_or_drop=kw.get("expect_all_or_drop") or {},
                expect_or_fail=kw.get("expect_all_or_fail") or {},
            )

        def create_auto_cdc_flow(target, source, keys, sequence_by, stored_as_scd_type=1, **kw):
            assert int(stored_as_scd_type) == 1, "local runtime only emulates SCD1"
            reg.datasets[target].cdc = {"source": source, "keys": keys, "sequence_by": sequence_by}

        m.create_streaming_table = create_streaming_table
        m.create_auto_cdc_flow = create_auto_cdc_flow
        return m


class _AutoLoaderReader:
    """readStream.format('cloudFiles') -> batch read with the same options."""

    def __init__(self, spark: SparkSession):
        self._spark, self._opts, self._schema = spark, {}, None

    def format(self, fmt):
        assert fmt == "cloudFiles", fmt
        return self

    def option(self, k, v):
        self._opts[k] = v
        return self

    def schema(self, s):
        self._schema = s
        return self

    def load(self, path):
        opts = dict(self._opts)
        fmt = opts.pop("cloudFiles.format")
        part_cols = opts.pop("cloudFiles.partitionColumns", None)
        for k in [k for k in opts if k.startswith("cloudFiles.")]:
            opts.pop(k)
        if fmt == "csv":
            opts.setdefault("inferSchema", "false")
        reader = self._spark.read.format(fmt).options(**opts)
        if self._schema is not None:
            reader = reader.schema(self._schema)
            if "rescuedDataColumn" in opts:  # make sure the column exists like Auto Loader would
                df = reader.load(path)
                return df if opts["rescuedDataColumn"] in df.columns else df.withColumn(opts["rescuedDataColumn"], F.lit(None).cast("string"))
        df = reader.load(path)
        if part_cols:
            for c in part_cols.split(","):
                assert c in df.columns, f"partition column {c} not discovered under {path}"
                df = df.withColumn(c, F.col(c).cast("string"))
        return df


class _TableReader:
    def __init__(self, runtime: "LocalPipeline", streaming: bool):
        self._rt, self._streaming = runtime, streaming

    def format(self, fmt):
        assert self._streaming
        return _AutoLoaderReader(self._rt.spark)

    def table(self, name):
        return self._rt.resolve(name)


class _FakeSpark:
    def __init__(self, runtime: "LocalPipeline", conf: dict):
        self._rt, self._conf = runtime, conf

    @property
    def readStream(self):
        return _TableReader(self._rt, streaming=True)

    @property
    def read(self):
        return _TableReader(self._rt, streaming=False)

    @property
    def conf(self):
        conf, real = self._conf, self._rt.spark.conf

        class _C:
            def get(self, k, default=None):
                return conf.get(k, default if default is not None else real.get(k, None))
        return _C()

    def __getattr__(self, item):
        return getattr(self._rt.spark, item)


class LocalPipeline:
    def __init__(self, spark: SparkSession, root: Path, conf: dict):
        self.spark, self.root, self.conf = spark, root, conf
        self.registry = Registry()
        self._cache: dict[str, DataFrame] = {}
        sys.modules["pyspark.pipelines"] = self.registry.module()
        import pyspark
        pyspark.pipelines = sys.modules["pyspark.pipelines"]
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))
        # Batch stand-in for streaming-only dedup: for a one-shot run it means the same thing.
        df_cls = type(spark.range(1))  # concrete class (pyspark.sql.classic.dataframe.DataFrame in Spark 4)
        if not getattr(df_cls, "_bkk_patched", False):
            _orig = df_cls.dropDuplicatesWithinWatermark

            def _dedup(df, subset=None):
                return df.dropDuplicates(subset) if not df.isStreaming else _orig(df, subset)

            df_cls.dropDuplicatesWithinWatermark = _dedup
            df_cls._bkk_patched = True
        self.fake_spark = _FakeSpark(self, conf)
        for f in sorted((root / "transformations").glob("*.py")):
            spec = importlib.util.spec_from_file_location(f"pipeline_{f.stem}", f)
            mod = importlib.util.module_from_spec(spec)
            mod.spark = self.fake_spark  # the runtime injects `spark` the same way
            spec.loader.exec_module(mod)

    def _apply_expectations(self, ds: Dataset, df: DataFrame) -> DataFrame:
        report = {}
        for name, cond in ds.expect_or_fail.items():
            bad = df.where(f"NOT ({cond}) OR ({cond}) IS NULL").count()
            assert bad == 0, f"{ds.name}: expectation {name} failed for {bad} rows"
        for name, cond in {**ds.expect, **ds.expect_or_drop}.items():
            report[name] = df.where(f"NOT coalesce({cond}, false)").count()
        for cond in ds.expect_or_drop.values():
            df = df.where(f"coalesce({cond}, false)")
        self.registry.quality[ds.name] = report
        return df

    def resolve(self, name: str) -> DataFrame:
        if name in self._cache:
            return self._cache[name]
        ds = self.registry.datasets[name]
        if ds.kind == "streaming_table":
            assert ds.cdc, f"{name} declared without a flow"
            src = self.resolve(ds.cdc["source"])
            seq = ds.cdc["sequence_by"]
            seq = F.col(seq) if isinstance(seq, str) else seq
            w = Window.partitionBy(*ds.cdc["keys"]).orderBy(seq.desc())
            df = src.withColumn("__rn", F.row_number().over(w)).where("__rn = 1").drop("__rn")
        else:
            df = ds.func()
        df = self._apply_expectations(ds, df)
        df = df.cache() if ds.kind != "view" else df
        self._cache[name] = df
        return df

    def run(self) -> dict[str, DataFrame]:
        return {n: self.resolve(n) for n, d in self.registry.datasets.items() if d.kind != "view"}
