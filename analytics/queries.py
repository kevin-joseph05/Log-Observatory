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

# Total row count across all 3.5M requests (sanity check before analytics)
con.sql("SELECT COUNT(*) FROM fact_requests").show()

# Week-over-week error rate trend: shows if the site is getting better or worse over time
weekly_q = """
        WITH weekly AS (
            SELECT 
                DATE_TRUNC('week', t.timestamp) AS week_start,
                COUNT(*) AS total, 
                SUM(CASE WHEN s.status IN ('4xx', '5xx') THEN 1 ELSE 0 END) AS errors
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
con.sql(weekly_q).show()
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
con.sql(p95_q).show()
con.execute(f"COPY ({p95_q}) TO '{project_root}/data/reports/p95_bytes.csv' (HEADER TRUE)")

# Top 10 worst endpoints by error rate (4xx/5xx as % of total requests)
top10_q = """
        SELECT 
            e.endpoint, e.method,
            COUNT(*) AS total,
            SUM(CASE WHEN s.status IN ('4xx', '5xx') THEN 1 ELSE 0 END) AS errors,
            ROUND(errors*1.0 / total, 4) AS error_rate
        FROM fact_requests as f
        JOIN dim_endpoint e ON f.endpt_key = e.endpt_key
        JOIN dim_status s ON f.status_key = s.status_key
        GROUP BY e.endpoint, e.method
        ORDER BY error_rate DESC
        LIMIT 10
        """
con.sql(top10_q).show()
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
con.sql(daily_q).show()
con.execute(f"COPY ({daily_q}) TO '{project_root}/data/reports/daily_active_endpoints.csv' (HEADER TRUE)")
