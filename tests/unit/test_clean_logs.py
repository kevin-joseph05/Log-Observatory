from pyspark.sql.types import IntegerType, TimestampType

from src.pipeline.clean_logs import CleanLogs


def _sample_df(spark):
    data = [
        ("host1", "01/Jul/1995:00:00:01 -0400", "GET", "/history/apollo/", "200", "6245"),
        ("host2", "02/Jul/1995:00:00:02 -0400", "POST", "/foo/bar?baz=1", "404", "-"),
        ("host3", "03/Jul/1995:00:01:03 -0400", "GET", "/shuttle/countdown/", "304", "0"),
        ("", "", "", "", "", ""),
    ]
    return spark.createDataFrame(
        data, ["host", "timestamp", "method", "endpoint", "status", "bytes"]
    )


def test_drop_failed_parse_removes_empty_rows(spark):
    df = _sample_df(spark)
    cl = CleanLogs(df)
    cl.drop_failed_parse()
    assert cl.df.count() == 3


def test_drop_failed_parse_keeps_valid_rows(spark):
    df = _sample_df(spark)
    cl = CleanLogs(df)
    cl.drop_failed_parse()
    hosts = {r.host for r in cl.df.collect()}
    assert hosts == {"host1", "host2", "host3"}


def test_normalize_bytes_converts_dash_to_zero(spark):
    df = _sample_df(spark)
    cl = CleanLogs(df)
    cl.normalize_bytes()
    rows = {r.host: r.bytes for r in cl.df.collect()}
    assert rows["host2"] == 0


def test_normalize_bytes_casts_to_int(spark):
    df = _sample_df(spark)
    cl = CleanLogs(df)
    cl.normalize_bytes()
    assert cl.df.schema["bytes"].dataType == IntegerType()


def test_normalize_bytes_preserves_numeric_values(spark):
    df = _sample_df(spark)
    cl = CleanLogs(df)
    cl.normalize_bytes()
    rows = {r.host: r.bytes for r in cl.df.collect()}
    assert rows["host1"] == 6245
    assert rows["host3"] == 0


def test_strip_query_removes_query_string(spark):
    df = _sample_df(spark)
    cl = CleanLogs(df)
    cl.strip_query()
    row = cl.df.filter(cl.df.host == "host2").collect()[0]
    assert row.endpoint == "/foo/bar"


def test_strip_query_leaves_noop(spark):
    df = _sample_df(spark)
    cl = CleanLogs(df)
    cl.strip_query()
    row = cl.df.filter(cl.df.host == "host1").collect()[0]
    assert row.endpoint == "/history/apollo/"


def test_cast_timestamp_converts_to_timestamptype(spark):
    df = _sample_df(spark)
    cl = CleanLogs(df)
    cl.cast_timestamp()
    assert cl.df.schema["timestamp"].dataType == TimestampType()


def test_cast_timestamp_parses_components(spark):
    df = _sample_df(spark)
    cl = CleanLogs(df)
    cl.cast_timestamp()
    row = cl.df.filter(cl.df.host == "host1").collect()[0]
    assert row.timestamp.year == 1995
    assert row.timestamp.month == 7
    assert row.timestamp.day == 1


def test_clean_parsed_df_full_chain(spark):
    df = _sample_df(spark)
    cl = CleanLogs(df)
    result = cl.clean_parsed_df()
    assert result.count() == 3
    assert result.schema["bytes"].dataType == IntegerType()
    assert result.schema["timestamp"].dataType == TimestampType()
    row = result.filter(result.host == "host2").collect()[0]
    assert row.endpoint == "/foo/bar"
