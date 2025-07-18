import psycopg2
from datetime import datetime, timedelta

DB_CONFIG = {
    'host': 'localhost',
    'database': 'fox_db',
    'user': 'gpu_user',
    'password': '',
    'port': '5432'
}

def check_data_for_date(target_date):
    """Check what data exists for a specific date"""
    print(f"🔍 Checking data for {target_date.strftime('%Y-%m-%d')}")
    print("=" * 50)
    
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) 
                FROM workstation_master_log 
                WHERE DATE(history_station_end_time) = %s
                    AND service_flow NOT IN ('NC Sort', 'RO')
                    AND service_flow IS NOT NULL
            """, (target_date,))
            
            total_records = cur.fetchone()[0]
            print(f"Total production records: {total_records}")
            
            if total_records == 0:
                print("No production data found for this date")
                return
            
            cur.execute("""
                SELECT model, COUNT(*) 
                FROM workstation_master_log 
                WHERE DATE(history_station_end_time) = %s
                    AND service_flow NOT IN ('NC Sort', 'RO')
                    AND service_flow IS NOT NULL
                GROUP BY model
                ORDER BY COUNT(*) DESC;
            """, (target_date,))
            
            models = cur.fetchall()
            print(f"\nRecords by model:")
            for model, count in models:
                print(f"  {model}: {count} records")
            
            cur.execute("""
                SELECT workstation_name, COUNT(*) 
                FROM workstation_master_log 
                WHERE DATE(history_station_end_time) = %s
                    AND service_flow NOT IN ('NC Sort', 'RO')
                    AND service_flow IS NOT NULL
                GROUP BY workstation_name
                ORDER BY COUNT(*) DESC
                LIMIT 10;
            """, (target_date,))
            
            stations = cur.fetchall()
            print(f"\nTop stations:")
            for station, count in stations:
                print(f"  {station}: {count} records")
            
            cur.execute("""
                SELECT COUNT(*) 
                FROM workstation_master_log 
                WHERE DATE(history_station_end_time) = %s
                    AND workstation_name = 'PACKING'
                    AND service_flow NOT IN ('NC Sort', 'RO')
                    AND service_flow IS NOT NULL
            """, (target_date,))
            
            packing_count = cur.fetchone()[0]
            print(f"\nPACKING station records: {packing_count}")
            
            cur.execute("""
                SELECT COUNT(DISTINCT sn) 
                FROM workstation_master_log 
                WHERE DATE(history_station_end_time) = %s
                    AND service_flow NOT IN ('NC Sort', 'RO')
                    AND service_flow IS NOT NULL
            """, (target_date,))
            
            unique_parts = cur.fetchone()[0]
            print(f"Unique parts: {unique_parts}")
            
    finally:
        conn.close()

def find_dates_with_data():
    """Find dates that have significant data"""
    print("Finding dates with significant data...")
    
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    DATE(history_station_end_time) as test_date,
                    COUNT(*) as record_count,
                    COUNT(DISTINCT sn) as unique_parts,
                    COUNT(CASE WHEN workstation_name = 'PACKING' THEN 1 END) as packing_records
                FROM workstation_master_log 
                WHERE history_station_end_time IS NOT NULL
                    AND service_flow NOT IN ('NC Sort', 'RO')
                    AND service_flow IS NOT NULL
                GROUP BY DATE(history_station_end_time)
                HAVING COUNT(*) >= 100
                ORDER BY record_count DESC
                LIMIT 10;
            """)
            
            results = cur.fetchall()
            print(f"\nTop dates with significant data:")
            for date, records, parts, packing in results:
                print(f"  {date}: {records} records, {parts} parts, {packing} packing")
                
            return [row[0] for row in results]
            
    finally:
        conn.close()

if __name__ == "__main__":
    test_date = datetime(2025, 6, 15).date() 
    check_data_for_date(test_date)
    
    test_date2 = datetime(2025, 6, 13).date() 
    print(f"\n" + "="*60)
    check_data_for_date(test_date2)
    
    print(f"\n" + "="*60)
    good_dates = find_dates_with_data()
    
    if good_dates:
        print(f"\nRecommended test date: {good_dates[0]}")
    else:
        print(f"\nNo dates with significant data found") 