#!/usr/bin/env python3
"""
Aggregates hourly part counts per station from workstation_master_log.
Groups by date, hour, and station, and prints the results.

-- SQL to create the summary table (optional) --
CREATE TABLE IF NOT EXISTS station_hourly_summary (
    date DATE NOT NULL,
    hour INTEGER NOT NULL,
    workstation_name TEXT NOT NULL,
    part_count INTEGER NOT NULL,
    PRIMARY KEY (date, hour, workstation_name)
);

Usage: python aggregate_station_hourly_counts.py
"""
import psycopg2
from datetime import datetime

DB_CONFIG = {
    'host': 'localhost',
    'database': 'fox_db',
    'user': 'gpu_user',
    'password': '',
    'port': '5432'
}

def aggregate_station_hourly_counts():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    DATE(history_station_end_time) AS date,
                    EXTRACT(HOUR FROM history_station_end_time)::int AS hour,
                    workstation_name,
                    COUNT(*) AS part_count
                FROM
                    workstation_master_log
                WHERE
                    history_station_end_time IS NOT NULL
                GROUP BY
                    DATE(history_station_end_time),
                    EXTRACT(HOUR FROM history_station_end_time),
                    workstation_name
                ORDER BY
                    date, hour, workstation_name;
            """)
            results = cur.fetchall()
            print(f"{'Date':<12} {'Hour':<4} {'Station':<16} {'Count':<6}")
            print("-" * 40)
            for date, hour, station, count in results:
                print(f"{date} {hour:>2}   {station:<16} {count:<6}")
    finally:
        conn.close()

if __name__ == "__main__":
    aggregate_station_hourly_counts() 