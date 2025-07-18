import psycopg2
from datetime import datetime, timedelta

DB_CONFIG = {
    'host': 'localhost',
    'database': 'fox_db',
    'user': 'gpu_user',
    'password': '',
    'port': '5432'
}

def check_required_fields():
    """Check if we have all required fields for TPY aggregation"""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'workstation_master_log'
                ORDER BY ordinal_position;
            """)
            
            columns = cur.fetchall()
            print("Available columns in workstation_master_log:")
            for col_name, data_type in columns:
                print(f"  {col_name}: {data_type}")
            
            required_fields = [
                'sn',                    
                'workstation_name',      
                'model',                 
                'history_station_end_time', 
                'history_station_passing_status', 
                'service_flow'           
            ]
            
            print(f"\nChecking required fields:")
            missing_fields = []
            for field in required_fields:
                found = any(col[0] == field for col in columns)
                status = "True" if found else "False"
                print(f"  {status} {field}")
                if not found:
                    missing_fields.append(field)
            
            if missing_fields:
                print(f"\nMissing required fields: {missing_fields}")
                return False
            else:
                print(f"\nAll required fields available!")
                return True
                
    finally:
        conn.close()

def check_data_quality():
    """Check data quality and sample values"""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            print(f"\nData Quality Check:")
            
            cur.execute("SELECT COUNT(*) FROM workstation_master_log")
            total_records = cur.fetchone()[0]
            print(f"  Total records: {total_records:,}")
            
            cur.execute("""
                SELECT 
                    MIN(history_station_end_time) as earliest_date,
                    MAX(history_station_end_time) as latest_date
                FROM workstation_master_log
                WHERE history_station_end_time IS NOT NULL
            """)
            date_range = cur.fetchone()
            print(f"  Date range: {date_range[0]} to {date_range[1]}")
            
            cur.execute("""
                SELECT model, COUNT(*) 
                FROM workstation_master_log 
                WHERE model IS NOT NULL
                GROUP BY model 
                ORDER BY COUNT(*) DESC
            """)
            models = cur.fetchall()
            print(f"  Models found:")
            for model, count in models:
                print(f"    {model}: {count:,} records")
            
            cur.execute("""
                SELECT service_flow, COUNT(*) 
                FROM workstation_master_log 
                WHERE service_flow IS NOT NULL
                    AND service_flow NOT IN ('NC Sort', 'RO')
                GROUP BY service_flow 
                ORDER BY COUNT(*) DESC
                LIMIT 10
            """)
            service_flows = cur.fetchall()
            print(f"  Top production service flows:")
            for flow, count in service_flows:
                print(f"    {flow}: {count:,} records")
            
            cur.execute("""
                SELECT workstation_name, COUNT(*) 
                FROM workstation_master_log 
                WHERE workstation_name IS NOT NULL
                    AND service_flow NOT IN ('NC Sort', 'RO')
                GROUP BY workstation_name 
                ORDER BY COUNT(*) DESC
                LIMIT 15
            """)
            stations = cur.fetchall()
            print(f"  Top stations (filtered):")
            for station, count in stations:
                print(f"    {station}: {count:,} records")
            
            cur.execute("""
                SELECT history_station_passing_status, COUNT(*) 
                FROM workstation_master_log 
                WHERE history_station_passing_status IS NOT NULL
                    AND service_flow NOT IN ('NC Sort', 'RO')
                GROUP BY history_station_passing_status
                ORDER BY COUNT(*) DESC
            """)
            statuses = cur.fetchall()
            print(f"  Pass/Fail status:")
            for status, count in statuses:
                print(f"    {status}: {count:,} records")
                
    finally:
        conn.close()

def test_sample_aggregation():
    """Test a sample aggregation to see if it works"""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            test_date = datetime.now().date() - timedelta(days=1)
            start_date = test_date
            end_date = test_date + timedelta(days=1)
            
            print(f"\nTesting sample aggregation for {test_date}:")
            
            cur.execute("""
                SELECT 
                    workstation_name,
                    model,
                    COUNT(*) as total_parts,
                    COUNT(CASE WHEN history_station_passing_status = 'Pass' THEN 1 END) as passed_parts,
                    COUNT(CASE WHEN history_station_passing_status != 'Pass' THEN 1 END) as failed_parts
                FROM workstation_master_log 
                WHERE history_station_end_time >= %s 
                    AND history_station_end_time < %s
                    AND service_flow NOT IN ('NC Sort', 'RO')
                    AND service_flow IS NOT NULL
                    AND model IN ('Tesla SXM4', 'Tesla SXM5')
                GROUP BY workstation_name, model
                HAVING COUNT(*) >= 5
                ORDER BY total_parts DESC
                LIMIT 10;
            """, (start_date, end_date))
            
            results = cur.fetchall()
            if results:
                print(f"Sample aggregation successful!")
                print(f"  Found {len(results)} station-model combinations")
                for station, model, total, passed, failed in results[:5]:
                    throughput_yield = (passed / total * 100) if total > 0 else 0
                    print(f"    {station} ({model}): {passed}/{total} = {throughput_yield:.1f}%")
            else:
                print(f"No data found for {test_date}")
                
    finally:
        conn.close()

def main():
    print("TPY Data Availability Check")
    print("=" * 50)
    
    fields_ok = check_required_fields()
    
    if fields_ok:
        check_data_quality()
        
        test_sample_aggregation()
        
        print(f"\nData verification complete!")
        print(f"Ready to build TPY aggregation system!")
    else:
        print(f"\nCannot proceed - missing required fields")

if __name__ == "__main__":
    main() 