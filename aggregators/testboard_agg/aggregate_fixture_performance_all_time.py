
import psycopg2
from psycopg2.extras import execute_values

DB_CONFIG = {
    'host': 'localhost',
    'database': 'fox_db',
    'user': 'gpu_user',
    'password': '',
    'port': '5432'
}

CREATE_TABLE_SQL = '''
CREATE TABLE IF NOT EXISTS fixture_performance_daily (
    day DATE NOT NULL,
    fixture_no TEXT NOT NULL,
    model TEXT,
    pn TEXT,
    workstation_name TEXT,
    pass INTEGER NOT NULL,
    fail INTEGER NOT NULL,
    total INTEGER NOT NULL,
    PRIMARY KEY (day, fixture_no, model, pn, workstation_name)
);
'''

AGGREGATE_SQL = '''
SELECT
    DATE(history_station_end_time) AS day,
    fixture_no,
    model,
    pn,
    workstation_name,
    COUNT(*) AS total,
    COUNT(CASE WHEN history_station_passing_status = 'Pass' THEN 1 END) AS pass,
    COUNT(CASE WHEN history_station_passing_status = 'Fail' THEN 1 END) AS fail
FROM testboard_master_log
WHERE history_station_end_time IS NOT NULL
GROUP BY day, fixture_no, model, pn, workstation_name
ORDER BY day DESC, fail DESC;
'''

INSERT_SQL = '''
INSERT INTO fixture_performance_daily (
    day, fixture_no, model, pn, workstation_name, pass, fail, total
) VALUES %s
ON CONFLICT (day, fixture_no, model, pn, workstation_name) DO UPDATE SET
    pass = EXCLUDED.pass,
    fail = EXCLUDED.fail,
    total = EXCLUDED.total;
'''

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            print("Creating fixture_performance_daily table if not exists...")
            cur.execute(CREATE_TABLE_SQL)
            conn.commit()

            print("Aggregating fixture performance data from testboard_master_log...")
            cur.execute(AGGREGATE_SQL)
            rows = cur.fetchall()
            print(f"Aggregated {len(rows)} rows.")

            if rows:
                values = [(
                    r[0], r[1], r[2], r[3], r[4], r[6], r[7], r[5]
                ) for r in rows]
                execute_values(cur, INSERT_SQL, values)
                conn.commit()
                print(" Fixture performance aggregation complete and upserted.")
            else:
                print("No data to aggregate.")
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main() 