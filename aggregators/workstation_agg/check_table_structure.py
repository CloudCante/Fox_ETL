import psycopg2

DB_CONFIG = {
    'host': 'localhost',
    'database': 'fox_db',
    'user': 'gpu_user',
    'password': '',
    'port': '5432'
}

def check_table_structure():
    """Check the actual structure of workstation_master_log table"""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_name = 'workstation_master_log'
                ORDER BY ordinal_position;
            """)
            
            columns = cur.fetchall()
            print("workstation_master_log table structure:")
            print("=" * 50)
            for col_name, data_type, is_nullable in columns:
                nullable = "NULL" if is_nullable == "YES" else "NOT NULL"
                print(f"  {col_name}: {data_type} ({nullable})")
            
            print(f"\nLooking for service flow columns:")
            service_flow_candidates = []
            for col_name, data_type, is_nullable in columns:
                if 'service' in col_name.lower() or 'flow' in col_name.lower():
                    service_flow_candidates.append(col_name)
                    print(f"  Found: {col_name}")
            
            if not service_flow_candidates:
                print("  No obvious service flow columns found")
                
            print(f"\nLooking for metadata columns:")
            metadata_candidates = []
            for col_name, data_type, is_nullable in columns:
                if 'metadata' in col_name.lower():
                    metadata_candidates.append(col_name)
                    print(f"  Found: {col_name}")
            
            if not metadata_candidates:
                print("  No metadata columns found")
                
    finally:
        conn.close()

def check_sample_data():
    """Check a sample row to understand the data structure"""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM workstation_master_log LIMIT 1;
            """)
            
            col_names = [desc[0] for desc in cur.description]
            row = cur.fetchone()
            
            print(f"\nSample row data:")
            print("=" * 30)
            for i, (col_name, value) in enumerate(zip(col_names, row)):
                print(f"  {col_name}: {value}")
                
    finally:
        conn.close()

def main():
    print("Checking workstation_master_log table structure")
    print("=" * 60)
    
    check_table_structure()
    check_sample_data()
    
    print(f"\nStructure check complete!")

if __name__ == "__main__":
    main() 