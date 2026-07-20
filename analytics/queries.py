import duckdb
from pathlib import Path
import sys


sys.path.insert(0, str(Path.cwd().parent))
project_root = Path.cwd()
con = duckdb.connect()
con.execute(f"""
    CREATE VIEW fact_requests AS
    SELECT * FROM read_parquet('{project_root}/data/curated/fact_requests/**/*.parquet')
""")

for dim in ['dim_endpoint', 'dim_host', 'dim_status', 'dim_timestamp']:
    con.execute(f"""
        CREATE VIEW {dim} AS
        SELECT * FROM read_parquet('{project_root}/data/curated/{dim}/*.parquet')
    """)

# Quick peek at raw August access log
raw_regex = r'^(\S+)\s+\S+\s+\S+\s+\[([^\]]+)\]\s+"(\S+)\s+(\S+)\s+\S+"\s+(\d{3})\s+(\d+|-)$'
con.sql(f"""
    SELECT
        regexp_extract(line, '{raw_regex}', 1) AS host,
        regexp_extract(line, '{raw_regex}', 2) AS timestamp,
        regexp_extract(line, '{raw_regex}', 3) AS method,
        regexp_extract(line, '{raw_regex}', 4) AS endpoint,
        regexp_extract(line, '{raw_regex}', 5) AS status,
        regexp_extract(line, '{raw_regex}', 6) AS bytes
    FROM read_csv('{project_root}/data/raw/access_log_Aug95',
                  auto_detect=false, sep='|', header=false,
                  columns={{'line': 'VARCHAR'}})
    WHERE regexp_extract(line, '{raw_regex}', 1) != ''
    LIMIT 20
""")

r = con.sql(f"""
        SELECT *
        FROM read_parquet('{project_root}/data/curated/dim_status/*.parquet');
        """)
print(r.columns)
for row in r.fetchall():
    print(row)
# Total row count across all 3.5M requests (sanity check before analytics)
con.sql("SELECT COUNT(*) FROM fact_requests")
# Week-over-week error rate trend: shows if the site is getting better or worse over time
weekly_q = """
        WITH weekly AS (
            SELECT 
                DATE_TRUNC('week', t.timestamp) AS week_start,
                COUNT(*) AS total, 
                SUM(CASE WHEN s.status >= 400 THEN 1 ELSE 0 END) AS errors
            FROM fact_requests as f
            JOIN dim_timestamp t ON f.timestamp_key = t.timestamp_key
            JOIN dim_status s ON f.status_key = s.status_key
            GROUP BY week_start
        )
        SELECT 
            week_start, total, errors,
            ROUND(errors*1.0 / total, 4) AS error_rate,
            ROUND((errors*1.0 / total) - LAG(errors*1.0 / total) OVER (ORDER BY week_start), 4 ) AS wow_change
        FROM weekly
        ORDER BY week_start
        """
con.sql(weekly_q)
con.execute(f"COPY ({weekly_q}) TO '{project_root}/data/reports/weekly_error_trend.csv' (HEADER TRUE)")

# P95 response bytes by endpoint category (e.g. "shuttle", "images", etc.)
p95_q = """
       SELECT 
            e.extracted AS endpoint_category,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY f.bytes) as p95_bytes
        FROM fact_requests as f
        JOIN dim_endpoint e ON f.endpt_key = e.endpt_key
        GROUP BY e.extracted
        """
con.sql(p95_q)
con.execute(f"COPY ({p95_q}) TO '{project_root}/data/reports/p95_bytes.csv' (HEADER TRUE)")

# Top 10 worst endpoints by error rate (4xx/5xx as % of total requests)
top10_q = """
        SELECT 
            e.endpoint, e.method,
            COUNT(*) AS total,
            SUM(CASE WHEN s.status >= 400 THEN 1 ELSE 0 END) AS errors,
            ROUND(errors*1.0 / total, 4) AS error_rate
        FROM fact_requests as f
        JOIN dim_endpoint e ON f.endpt_key = e.endpt_key
        JOIN dim_status s ON f.status_key = s.status_key
        GROUP BY e.endpoint, e.method
        ORDER BY error_rate DESC
        LIMIT 10
        """
con.sql(top10_q)
con.execute(f"COPY ({top10_q}) TO '{project_root}/data/reports/top10_errors.csv' (HEADER TRUE)")

# Daily active endpoints: how many unique URLs were hit each day
daily_q = """
        SELECT 
            DATE_TRUNC('day', t.timestamp) AS day2,
            COUNT(DISTINCT e.endpoint) AS active_endpoints
        FROM fact_requests as f
        JOIN dim_timestamp t ON f.timestamp_key = t.timestamp_key
        JOIN dim_endpoint e ON f.endpt_key = e.endpt_key
        GROUP BY day2
        ORDER BY day2
        """
con.sql(daily_q)
con.execute(f"COPY ({daily_q}) TO '{project_root}/data/reports/daily_active_endpoints.csv' (HEADER TRUE)")
