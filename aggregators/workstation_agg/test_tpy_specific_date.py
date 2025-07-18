import psycopg2
from datetime import datetime, timedelta
from aggregate_tpy_daily import aggregate_daily_tpy_for_date

DB_CONFIG = {
    'host': 'localhost',
    'database': 'fox_db',
    'user': 'gpu_user',
    'password': '',
    'port': '5432'
}

def test_specific_date():
    """Test TPY aggregation for a specific date from the data range"""
    test_date = datetime(2025, 5, 14).date()  
    
    print(f"Testing TPY aggregation for {test_date.strftime('%Y-%m-%d')}")
    print("=" * 60)
    
    try:
        result = aggregate_daily_tpy_for_date(test_date)
        print(f"\nTest completed successfully!")
        print(f"Inserted/Updated: {result['insertedCount']} records")
        print(f"Daily FPY: {result['dailyFPY']:.1f}%")
        print(f"Completed today: {result['completedToday']} parts")
        
        conn = psycopg2.connect(**DB_CONFIG)
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT model, workstation_name, total_parts, passed_parts, throughput_yield
                    FROM daily_tpy_metrics 
                    WHERE date_id = %s
                    ORDER BY model, throughput_yield DESC
                    LIMIT 10;
                """, (test_date,))
                
                results = cur.fetchall()
                print(f"\nSample results for {test_date}:")
                for model, station, total, passed, yield_pct in results:
                    print(f"  {model} {station}: {passed}/{total} = {yield_pct:.1f}%")
                    
        finally:
            conn.close()
            
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_specific_date() 