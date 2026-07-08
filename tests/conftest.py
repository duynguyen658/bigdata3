from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

# On Windows, Spark's local-filesystem reads shard out to Hadoop's NativeIO,
# which needs winutils.exe/hadoop.dll discoverable on PATH (HADOOP_HOME alone
# is not enough -- Windows resolves DLLs via PATH, not the env var). This is a
# no-op when HADOOP_HOME isn't set (Linux/Mac) or the bin dir doesn't exist.
_hadoop_home = os.environ.get("HADOOP_HOME")
if _hadoop_home:
    _hadoop_bin = os.path.join(_hadoop_home, "bin")
    if os.path.isdir(_hadoop_bin) and _hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = _hadoop_bin + os.pathsep + os.environ.get("PATH", "")


@pytest.fixture(scope="session")
def spark_session():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("aqi-tests")
        .master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
