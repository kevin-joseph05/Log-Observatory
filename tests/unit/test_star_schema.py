from datetime import datetime

from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.schema.star_schema import StarSchema


def _sample_df(spark):
    """Mimics the output of CleanLogs.clean_parsed_df()."""
    schema = StructType([
        StructField("host", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("method", StringType()),
        StructField("endpoint", StringType()),
        StructField("status", StringType()),
        StructField("bytes", IntegerType()),
    ])
    data = [
        ("host1", datetime(1995, 7, 1, 0, 0, 1), "GET", "/history/apollo/", "200", 6245),
        ("host2", datetime(1995, 7, 1, 0, 0, 2), "POST", "/foo/bar", "404", 0),
        ("host1", datetime(1995, 7, 2, 12, 30, 0), "GET", "/history/apollo/", "200", 1000),
    ]
    return spark.createDataFrame(data, schema)


class TestBuildTimeDimension:
    def test_selects_distinct_timestamps(self, spark):
        ss = StarSchema(_sample_df(spark))
        ss.build_time_dimension()
        assert ss.dim_timestamp.count() == 3

    def test_creates_derived_columns(self, spark):
        ss = StarSchema(_sample_df(spark))
        ss.build_time_dimension()
        expected = {"timestamp", "day_of_week", "hour", "year", "month", "day", "timestamp_key"}
        assert expected.issubset(ss.dim_timestamp.columns)

    def test_extracts_correct_year(self, spark):
        ss = StarSchema(_sample_df(spark))
        ss.build_time_dimension()
        rows = {r.timestamp: r.year for r in ss.dim_timestamp.collect()}
        assert all(v == 1995 for v in rows.values())

    def test_extracts_correct_month(self, spark):
        ss = StarSchema(_sample_df(spark))
        ss.build_time_dimension()
        rows = {r.timestamp: r.month for r in ss.dim_timestamp.collect()}
        assert all(v == 7 for v in rows.values())

    def test_hour_returns_0_for_midnight(self, spark):
        ss = StarSchema(_sample_df(spark))
        ss.build_time_dimension()
        ts = datetime(1995, 7, 1, 0, 0, 1)
        row = ss.dim_timestamp.filter(ss.dim_timestamp.timestamp == ts).collect()[0]
        assert row.hour == 0

    def test_hour_returns_12_for_afternoon(self, spark):
        ss = StarSchema(_sample_df(spark))
        ss.build_time_dimension()
        ts = datetime(1995, 7, 2, 12, 30, 0)
        row = ss.dim_timestamp.filter(ss.dim_timestamp.timestamp == ts).collect()[0]
        assert row.hour == 12


class TestBuildStatusDimension:
    def test_bins_200_as_2xx(self, spark):
        ss = StarSchema(_sample_df(spark))
        ss.build_status_dimension()
        rows = ss.dim_status.collect()
        for r in rows:
            if r.endpoint == "/history/apollo/":
                assert r.status == "2xx"

    def test_bins_404_as_4xx(self, spark):
        ss = StarSchema(_sample_df(spark))
        ss.build_status_dimension()
        row = ss.dim_status.filter(ss.dim_status.endpoint == "/foo/bar").collect()[0]
        assert row.status == "4xx"

    def test_adds_message_from_descriptions(self, spark):
        ss = StarSchema(_sample_df(spark))
        ss.build_status_dimension()
        row = ss.dim_status.filter(ss.dim_status.endpoint == "/foo/bar").collect()[0]
        assert row.message is not None

    def test_has_surrogate_key(self, spark):
        ss = StarSchema(_sample_df(spark))
        ss.build_status_dimension()
        assert "status_key" in ss.dim_status.columns


class TestBuildEndpointDimension:
    def test_selects_distinct_endpoints(self, spark):
        ss = StarSchema(_sample_df(spark))
        ss.build_endpoint_dimension()
        assert ss.dim_endpoint.count() == 2

    def test_creates_extracted_column(self, spark):
        ss = StarSchema(_sample_df(spark))
        ss.build_endpoint_dimension()
        endpoints = {r.endpoint: r.extracted for r in ss.dim_endpoint.collect()}
        assert endpoints["/history/apollo/"] == "history"
        assert endpoints["/foo/bar"] == "foo"

    def test_has_surrogate_key(self, spark):
        ss = StarSchema(_sample_df(spark))
        ss.build_endpoint_dimension()
        assert "endpt_key" in ss.dim_endpoint.columns


class TestBuildHostTable:
    def test_selects_distinct_hosts(self, spark):
        ss = StarSchema(_sample_df(spark))
        ss.build_host_table()
        assert ss.dim_host.count() == 2

    def test_has_surrogate_key(self, spark):
        ss = StarSchema(_sample_df(spark))
        ss.build_host_table()
        assert "host_key" in ss.dim_host.columns


class TestStubs:
    def test_build_fact_table_is_stub(self, spark):
        ss = StarSchema(_sample_df(spark))
        ss.build_fact_table()
        assert ss.fact_requests is None

    def test_build_is_stub(self, spark):
        ss = StarSchema(_sample_df(spark))
        ss.build()
        assert ss.fact_requests is None
        assert ss.dim_timestamp is None
