#!/usr/bin/env python3
"""
Upload all testboard Excel files into testboard_master_log with clean schema.
"""
import psycopg2
import pandas as pd
import glob
import os
from psycopg2.extras import execute_values

def connect_to_db():
    print("🔌 Attempting to connect to database...")
    return psycopg2.connect(
        host="localhost",
        database="fox_db",
        user="gpu_user",
        password="",
        port="5432"
    )

def create_testboard_table(conn):
    print("📝 Creating/verifying testboard table...")
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS testboard_master_log (
        id SERIAL PRIMARY KEY,
        sn VARCHAR(255) NOT NULL,
        pn VARCHAR(255),
        model VARCHAR(255),
        work_station_process VARCHAR(255),
        baseboard_sn VARCHAR(255),
        baseboard_pn VARCHAR(255),
        workstation_name VARCHAR(255) NOT NULL,
        history_station_start_time TIMESTAMP NOT NULL,
        history_station_end_time TIMESTAMP NOT NULL,
        history_station_passing_status VARCHAR(255),
        operator VARCHAR(255),
        failure_reasons TEXT,
        failure_note TEXT,
        failure_code VARCHAR(255),
        diag_version VARCHAR(255),
        fixture_no VARCHAR(255),
        data_source VARCHAR(50) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    
    # Add unique constraint separately
    try:
        cursor.execute("""
        ALTER TABLE testboard_master_log 
        ADD CONSTRAINT testboard_unique_constraint 
        UNIQUE (sn, pn, model, work_station_process, baseboard_sn, baseboard_pn, workstation_name,
                history_station_start_time, history_station_end_time, history_station_passing_status, operator,
                failure_reasons, failure_note, failure_code, diag_version, fixture_no, data_source);
        """)
    except Exception as e:
        # Constraint might already exist
        print(f"Note: Unique constraint may already exist: {e}")
    
    conn.commit()
    cursor.close()

def clean_column_name(col_name):
    """Clean column names for PostgreSQL compatibility"""
    cleaned = col_name.lower().replace(' ', '_').replace('-', '_')
    cleaned = ''.join(c for c in cleaned if c.isalnum() or c == '_')
    return cleaned

def convert_timestamp(value):
    """Convert pandas Timestamp to Python datetime"""
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return pd.to_datetime(value)

def convert_empty_string(value):
    """Convert empty strings to None for proper comparison"""
    if isinstance(value, str) and value.strip() == '':
        return None
    return value

def main():
    print("🚀 Starting testboard data upload process...")
    
    try:
        conn = connect_to_db()
        print("✅ Database connection successful")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return
        
    create_testboard_table(conn)
    
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"📂 Script directory: {script_dir}")
    
    # Build path to Excel files relative to script location
    excel_path = os.path.join(script_dir, "input", "data log", "testboardrecord_xlsx", "**", "*.xlsx")
    print(f"🔍 Looking for Excel files in: {excel_path}")
    
    # Use glob with normalized path
    excel_path_normalized = os.path.normpath(excel_path)
    print(f"🔍 Normalized path: {excel_path_normalized}")
    
    testboard_files = glob.glob(excel_path_normalized, recursive=True)
    print(f"📊 Found {len(testboard_files)} Excel files")
    
    if not testboard_files:
        print("\n❌ No Excel files found! Checking directory existence:")
        check_path = os.path.join(script_dir, "input", "data log", "testboardrecord_xlsx")
        if os.path.exists(check_path):
            print(f"✅ Directory exists: {check_path}")
            print("📂 Contents:")
            for root, dirs, files in os.walk(check_path):
                print(f"\nDirectory: {root}")
                if dirs:
                    print("Subdirectories:", dirs)
                if files:
                    print("Files:", files)
        else:
            print(f"❌ Directory does not exist: {check_path}")
        return
        
    total_imported = 0
    
    for i, file_path in enumerate(testboard_files, 1):
        print(f"\nProcessing file {i}/{len(testboard_files)}: {os.path.basename(file_path)}")
        
        try:
            # Read Excel file
            print(f"📖 Reading file: {file_path}")
            df = pd.read_excel(file_path)
            print(f"✅ Successfully read file with {len(df)} rows")
            
            # Clean column names
            df.columns = [clean_column_name(col) for col in df.columns]
            
            # Map columns to our clean schema
            mapped_data = []
            for _, row in df.iterrows():
                mapped_row = {
                    'sn': convert_empty_string(str(row.get('sn', ''))),
                    'pn': convert_empty_string(str(row.get('pn', ''))),
                    'model': convert_empty_string(str(row.get('model', ''))),
                    'work_station_process': convert_empty_string(str(row.get('work_station_process', ''))),
                    'baseboard_sn': convert_empty_string(str(row.get('baseboard_sn', ''))),
                    'baseboard_pn': convert_empty_string(str(row.get('baseboard_pn', ''))),
                    'workstation_name': convert_empty_string(str(row.get('workstation_name', ''))),
                    'history_station_start_time': convert_timestamp(row.get('history_station_start_time')),
                    'history_station_end_time': convert_timestamp(row.get('history_station_end_time')),
                    'history_station_passing_status': convert_empty_string(str(row.get('history_station_passing_status', ''))),
                    'operator': convert_empty_string(str(row.get('operator', ''))),
                    'failure_reasons': convert_empty_string(str(row.get('failure_reasons', ''))),
                    'failure_note': convert_empty_string(str(row.get('failure_note', ''))),
                    'failure_code': convert_empty_string(str(row.get('failure_code', ''))),
                    'diag_version': convert_empty_string(str(row.get('diag_version', ''))),
                    'fixture_no': convert_empty_string(str(row.get('fixture_no', ''))),
                    'data_source': 'testboard'
                }
                mapped_data.append(mapped_row)
            
            # Insert into database
            cursor = conn.cursor()
            
            insert_query = """
            INSERT INTO testboard_master_log (
                sn, pn, model, work_station_process, baseboard_sn, baseboard_pn, workstation_name,
                history_station_start_time, history_station_end_time, history_station_passing_status, operator,
                failure_reasons, failure_note, failure_code, diag_version, fixture_no, data_source
            ) VALUES %s
            ON CONFLICT ON CONSTRAINT testboard_unique_constraint
            DO NOTHING
            """
            
            # Prepare data for bulk insert
            values = [(
                row['sn'], row['pn'], row['model'], row['work_station_process'], row['baseboard_sn'], row['baseboard_pn'], row['workstation_name'],
                row['history_station_start_time'], row['history_station_end_time'], row['history_station_passing_status'], row['operator'],
                row['failure_reasons'], row['failure_note'], row['failure_code'], row['diag_version'], row['fixture_no'], row['data_source']
            ) for row in mapped_data]
            
            execute_values(cursor, insert_query, values)
            conn.commit()
            cursor.close()
            
            file_imported = len(mapped_data)
            total_imported += file_imported
            print(f"  ✅ Imported {file_imported:,} records from {os.path.basename(file_path)}")
            
        except Exception as e:
            print(f"  ❌ Error importing {os.path.basename(file_path)}: {e}")
            conn.rollback()
            continue
    
    print(f"\n📊 Total testboard records imported: {total_imported:,}")
    conn.close()

if __name__ == "__main__":
    main() 