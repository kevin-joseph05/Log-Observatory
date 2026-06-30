import tempfile
from pathlib import Path

from src.pipeline.parse_logs import parse_raw_log

SAMPLE_LINES = [
    '199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245\n',
    'burger.letters.com - - [01/Jul/1995:00:00:11 -0400] "GET /shuttle/countdown/liftoff.html HTTP/1.0" 304 0\n',
    'd104.aa.net - - [01/Jul/1995:00:01:52 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 -\n',
    '199.120.110.21 - - [01/Jul/1995:00:00:09 -0400] "GET /shuttle/missions/sts-73/mission-sts-73.html?foo=bar HTTP/1.0" 200 4085\n',
    "corrupt-line-no-brackets\n",
]


def test_parse_raw_log_parses_normal_lines(spark):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "sample.log"
        path.write_text("".join(SAMPLE_LINES))

        df = parse_raw_log([str(path)])
        rows = df.collect()

    assert len(rows) == 5


def test_parse_raw_log_extracts_host(spark):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "sample.log"
        path.write_text(SAMPLE_LINES[0])

        df = parse_raw_log([str(path)])
        row = df.collect()[0]

    assert row.host == "199.72.81.55"


def test_parse_raw_log_extracts_timestamp(spark):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "sample.log"
        path.write_text(SAMPLE_LINES[0])

        df = parse_raw_log([str(path)])
        row = df.collect()[0]

    assert row.timestamp == "01/Jul/1995:00:00:01 -0400"


def test_parse_raw_log_extracts_method(spark):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "sample.log"
        path.write_text(SAMPLE_LINES[0])

        df = parse_raw_log([str(path)])
        row = df.collect()[0]

    assert row.method == "GET"


def test_parse_raw_log_extracts_endpoint(spark):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "sample.log"
        path.write_text(SAMPLE_LINES[0])

        df = parse_raw_log([str(path)])
        row = df.collect()[0]

    assert row.endpoint == "/history/apollo/"


def test_parse_raw_log_extracts_endpoint_with_query(spark):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "sample.log"
        path.write_text(SAMPLE_LINES[3])

        df = parse_raw_log([str(path)])
        row = df.collect()[0]

    assert row.endpoint == "/shuttle/missions/sts-73/mission-sts-73.html?foo=bar"


def test_parse_raw_log_extracts_status(spark):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "sample.log"
        path.write_text(SAMPLE_LINES[0])

        df = parse_raw_log([str(path)])
        row = df.collect()[0]

    assert row.status == "200"


def test_parse_raw_log_extracts_bytes(spark):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "sample.log"
        path.write_text(SAMPLE_LINES[1])

        df = parse_raw_log([str(path)])
        row = df.collect()[0]

    assert row.bytes == "0"


def test_parse_raw_log_handles_missing_bytes(spark):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "sample.log"
        path.write_text(SAMPLE_LINES[2])

        df = parse_raw_log([str(path)])
        row = df.collect()[0]

    assert row.bytes == "-"


def test_parse_raw_log_returns_string_types(spark):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "sample.log"
        path.write_text("".join(SAMPLE_LINES[:1]))

        df = parse_raw_log([str(path)])
        types = dict(df.dtypes)

    assert types["host"] == "string"
    assert types["timestamp"] == "string"
    assert types["method"] == "string"
    assert types["endpoint"] == "string"
    assert types["status"] == "string"
    assert types["bytes"] == "string"


def test_parse_raw_log_handles_corrupt_lines(spark):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "sample.log"
        path.write_text(SAMPLE_LINES[4])

        df = parse_raw_log([str(path)])
        row = df.collect()[0]

    assert row.host == ""
    assert row.timestamp == ""
    assert row.method == ""
    assert row.endpoint == ""
    assert row.status == ""
    assert row.bytes == ""
