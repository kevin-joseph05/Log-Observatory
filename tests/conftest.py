import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .appName("test")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield s
    s.stop()
