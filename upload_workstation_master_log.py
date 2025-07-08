#!/usr/bin/env python3
"""
Upload all workstation Excel files into workstation_master_log with clean schema.
"""
import psycopg2
import pandas as pd
import glob
import os
from psycopg2.extras import execute_values

def connect_to_db():
    return psycopg2.connect(
        host="localhost",
        database="fox_db",
        user="gpu_user",
        password="",
        port="5432"
    )

def create_workstation_table(conn):
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS workstation_master_log (
        id SERIAL PRIMARY KEY,
        sn VARCHAR(255) NOT NULL,
        pn VARCHAR(255),
        model VARCHAR(255),
        workstation_name VARCHAR(255) NOT NULL,
        history_station_start_time TIMESTAMP NOT NULL,
        history_station_end_time TIMESTAMP NOT NULL,
        history_station_passing_status VARCHAR(255),
        operator VARCHAR(255),
        customer_pn VARCHAR(255),
        outbound_version VARCHAR(255),
        hours VARCHAR(255),
        service_flow VARCHAR(255),
        passing_station_method VARCHAR(255),
        first_station_start_time TIMESTAMP,
        data_source VARCHAR(50) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    
    # Add unique constraint separately
    try:
        cursor.execute("""
        ALTER TABLE workstation_master_log 
        ADD CONSTRAINT workstation_unique_constraint 
        UNIQUE (sn, pn, customer_pn, outbound_version, workstation_name,
                history_station_start_time, history_station_end_time, hours,
                service_flow, model, history_station_passing_status,
                passing_station_method, operator, first_station_start_time, data_source);
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
    print("🚀 Uploading workstation data to workstation_master_log...")
    conn = connect_to_db()
    create_workstation_table(conn)
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    excel_path = os.path.join(script_dir, "input", "data log", "workstationreport_xlsx", "**", "*.xlsx")
    workstation_files = glob.glob(os.path.normpath(excel_path), recursive=True)
    
    if not workstation_files:
        print("⚠️ No Excel files found in:", os.path.dirname(excel_path))
        return
        
    total_imported = 0
    
    for i, file_path in enumerate(workstation_files, 1):
        print(f"Processing file {i}/{len(workstation_files)}: {os.path.basename(file_path)}")
        
        try:
            # Read Excel file
            df = pd.read_excel(file_path)
            
            # Clean column names
            df.columns = [clean_column_name(col) for col in df.columns]
            
            # Map columns to our clean schema
            mapped_data = []
            for _, row in df.iterrows():
                mapped_row = {
                    'sn': convert_empty_string(str(row.get('sn', ''))),
                    'pn': convert_empty_string(str(row.get('pn', ''))),
                    'model': convert_empty_string(str(row.get('model', ''))),
                    'workstation_name': convert_empty_string(str(row.get('workstation_name', ''))),
                    'history_station_start_time': convert_timestamp(row.get('history_station_start_time')),
                    'history_station_end_time': convert_timestamp(row.get('history_station_end_time')),
                    'history_station_passing_status': convert_empty_string(str(row.get('history_station_passing_status', ''))),
                    'operator': convert_empty_string(str(row.get('operator', ''))),
                    'customer_pn': convert_empty_string(str(row.get('customer_pn', ''))),
                    'outbound_version': convert_empty_string(str(row.get('outbound_version', ''))),
                    'hours': convert_empty_string(str(row.get('hours', ''))),
                    'service_flow': convert_empty_string(str(row.get('service_flow', ''))),
                    'passing_station_method': convert_empty_string(str(row.get('passing_station_method', ''))),
                    'first_station_start_time': convert_timestamp(row.get('first_station_start_time')),
                    'data_source': 'workstation'
                }
                mapped_data.append(mapped_row)
            
            # Insert into database
            cursor = conn.cursor()
            
            insert_query = """
            INSERT INTO workstation_master_log (
                sn, pn, model, workstation_name, history_station_start_time, history_station_end_time,
                history_station_passing_status, operator, customer_pn, outbound_version, hours,
                service_flow, passing_station_method, first_station_start_time, data_source
            ) VALUES %s
            ON CONFLICT ON CONSTRAINT workstation_unique_constraint
            DO NOTHING
            """
            
            # Prepare data for bulk insert
            values = [(
                row['sn'], row['pn'], row['model'], row['workstation_name'], row['history_station_start_time'], row['history_station_end_time'],
                row['history_station_passing_status'], row['operator'], row['customer_pn'], row['outbound_version'], row['hours'],
                row['service_flow'], row['passing_station_method'], row['first_station_start_time'], row['data_source']
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
    
    print(f"\n📊 Total workstation records imported: {total_imported:,}")
    conn.close()

if __name__ == "__main__":
    main() 