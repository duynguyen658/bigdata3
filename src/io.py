from __future__ import annotations

from pathlib import Path
import shutil
import tempfile

import pandas as pd


MEASUREMENT_COLUMNS = [
    "sensor_id",
    "location_id",
    "location_name",
    "datetime_utc",
    "datetime_local",
    "latitude",
    "longitude",
    "parameter",
    "unit",
    "value",
    "source",
]

MEASUREMENT_ID_COLUMNS = ["sensor_id", "parameter", "datetime_utc"]
PARTITION_COLUMNS = ["parameter", "date"]


def _normalize_measurements(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    for column in MEASUREMENT_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    frame = frame[MEASUREMENT_COLUMNS]
    parameter = frame["parameter"].astype("string").str.strip().str.lower().str.replace(".", "", regex=False)
    frame["parameter"] = parameter.mask(parameter.isin(["", "nan", "none", "null"]))
    utc = pd.to_datetime(frame["datetime_utc"], utc=True, errors="coerce")
    frame["datetime_utc"] = utc.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    frame["date"] = utc.dt.strftime("%Y-%m-%d")
    frame = frame.dropna(subset=["sensor_id", "parameter", "datetime_utc", "date"])
    return frame


def _deduplicate_measurements(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.drop_duplicates(subset=MEASUREMENT_ID_COLUMNS, keep="last").sort_values(
        ["date", "parameter", "datetime_utc", "sensor_id"]
    )


def _write_local_measurements(frame: pd.DataFrame, path: str, mode: str) -> int:
    target = Path(path)
    if mode not in {"append", "overwrite"}:
        raise ValueError("mode must be 'append' or 'overwrite'")
    if mode == "append" and target.exists():
        existing = pd.read_parquet(target)
        frame = pd.concat([existing, frame], ignore_index=True)
    frame = _deduplicate_measurements(frame)

    tmp = target.with_name(f".{target.name}.tmp")
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(tmp, index=False, partition_cols=PARTITION_COLUMNS)
    if target.exists():
        shutil.rmtree(target)
    tmp.rename(target)
    return len(frame)


def _spark_filesystem_and_path(spark, path: str):
    hadoop_conf = spark._jsc.hadoopConfiguration()
    uri = spark._jvm.java.net.URI.create(path)
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
    return fs, spark._jvm.org.apache.hadoop.fs.Path(path)


def _spark_path_exists(spark, path: str) -> bool:
    fs, hadoop_path = _spark_filesystem_and_path(spark, path)
    return bool(fs.exists(hadoop_path))


def _combine_spark_measurements_for_write(spark, batch, path: str, mode: str):
    if mode == "append":
        if not _spark_path_exists(spark, path):
            return batch
        existing = spark.read.parquet(path)
        return existing.unionByName(batch, allowMissingColumns=True)
    if mode == "overwrite":
        return batch
    raise ValueError("mode must be 'append' or 'overwrite'")


def _write_spark_measurements(frame: pd.DataFrame, path: str, mode: str) -> int:
    from pyspark.sql import SparkSession, Window
    from pyspark.sql import functions as F

    spark = SparkSession.getActiveSession() or SparkSession.builder.appName("measurement-storage").getOrCreate()
    with tempfile.TemporaryDirectory(prefix="aqi-measurements-") as temp_dir:
        batch_path = Path(temp_dir) / "batch.parquet"
        frame.to_parquet(batch_path, index=False)
        batch = spark.read.parquet(batch_path.as_posix())
        combined = _combine_spark_measurements_for_write(spark, batch, path, mode)

        w = Window.partitionBy(*MEASUREMENT_ID_COLUMNS).orderBy(F.col("datetime_utc").desc())
        deduped = combined.withColumn("_rank", F.row_number().over(w)).where("_rank = 1").drop("_rank")
        count = deduped.count()

        tmp_path = f"{path.rstrip('/')}_tmp_write"
        deduped.write.mode("overwrite").partitionBy(*PARTITION_COLUMNS).parquet(tmp_path)

        fs, final = _spark_filesystem_and_path(spark, path)
        tmp = spark._jvm.org.apache.hadoop.fs.Path(tmp_path)
        if fs.exists(final):
            fs.delete(final, True)
        fs.rename(tmp, final)
        return count


def write_measurements_parquet(df: pd.DataFrame, path: str, mode: str = "append") -> int:
    """Write measurement rows using append + dedup semantics.

    Stable identity is `sensor_id + parameter + datetime_utc`, matching the
    actual measurement schema. Local paths are handled with pandas/pyarrow.
    `hdfs://` and other Spark-supported paths are routed through Spark so the
    write path remains Hadoop-compatible.
    """
    frame = _normalize_measurements(df)
    if path.startswith("hdfs://"):
        return _write_spark_measurements(frame, path, mode=mode)
    return _write_local_measurements(frame, path, mode=mode)


def read_predictions_json(path: str) -> list[dict]:
    import json

    target = Path(path)
    if not target.exists():
        return []
    return json.loads(target.read_text(encoding="utf-8"))
