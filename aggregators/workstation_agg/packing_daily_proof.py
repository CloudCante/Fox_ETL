import psycopg2

DB_CONFIG = {
    'host': 'localhost',
    'database': 'fox_db',
    'user': 'gpu_user',
    'password': '',
    'port': '5432'
}

TARGET_DATE = '2025-06-23'  


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute('''
                SELECT COUNT(*) FROM workstation_master_log
                WHERE DATE(history_station_end_time) = %s
                  AND history_station_passing_status = 'Pass'
                  AND workstation_name = 'PACKING';
            ''', (TARGET_DATE,))
            total_packed = cur.fetchone()[0]
            print(f"Total packed units on {TARGET_DATE}: {total_packed}")

            cur.execute('''
                SELECT model, COUNT(*) FROM workstation_master_log
                WHERE DATE(history_station_end_time) = %s
                  AND history_station_passing_status = 'Pass'
                  AND workstation_name = 'PACKING'
                GROUP BY model
                ORDER BY model;
            ''', (TARGET_DATE,))
            print(f"\nPacked by model on {TARGET_DATE}:")
            for model, count in cur.fetchall():
                print(f"  {model}: {count}")

            cur.execute('''
                SELECT model, pn, COUNT(*) FROM workstation_master_log
                WHERE DATE(history_station_end_time) = %s
                  AND history_station_passing_status = 'Pass'
                  AND workstation_name = 'PACKING'
                GROUP BY model, pn
                ORDER BY model, pn;
            ''', (TARGET_DATE,))
            print(f"\nPacked by part number within each model on {TARGET_DATE}:")
            for model, pn, count in cur.fetchall():
                print(f"  Model: {model} | Part Number: {pn} | Count: {count}")
    finally:
        conn.close()

if __name__ == "__main__":
    main() 