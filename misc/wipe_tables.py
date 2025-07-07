#!/usr/bin/env python3
"""
Table Wipe Script
Completely clears both master_records and workstation_master_log tables
"""

import psycopg2
import time

def connect_to_db():
    """Connect to PostgreSQL database"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="fox_db",
            user="gpu_user",
            password="",
            port="5432"
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def wipe_master_records():
    """Wipe the master_records table"""
    conn = connect_to_db()
    cursor = conn.cursor()
    
    try:
        print("🧹 Wiping master_records table...")
        
        # Get initial count
        cursor.execute("SELECT COUNT(*) FROM master_records")
        initial_count = cursor.fetchone()[0]
        print(f"📊 Initial record count: {initial_count:,}")
        
        # Delete all records
        cursor.execute("DELETE FROM master_records")
        deleted_count = cursor.rowcount
        conn.commit()
        
        # Get final count
        cursor.execute("SELECT COUNT(*) FROM master_records")
        final_count = cursor.fetchone()[0]
        
        print(f"🗑️ Deleted {deleted_count:,} records")
        print(f"📊 Final record count: {final_count:,}")
        print(f"✅ master_records table wiped!")
        
        return deleted_count
        
    except Exception as e:
        print(f"❌ Error wiping master_records: {e}")
        conn.rollback()
        return 0
    finally:
        cursor.close()
        conn.close()

def wipe_workstation_master_log():
    """Wipe the workstation_master_log table"""
    conn = connect_to_db()
    cursor = conn.cursor()
    
    try:
        print("🧹 Wiping workstation_master_log table...")
        
        # Get initial count
        cursor.execute("SELECT COUNT(*) FROM workstation_master_log")
        initial_count = cursor.fetchone()[0]
        print(f"📊 Initial record count: {initial_count:,}")
        
        # Delete all records
        cursor.execute("DELETE FROM workstation_master_log")
        deleted_count = cursor.rowcount
        conn.commit()
        
        # Get final count
        cursor.execute("SELECT COUNT(*) FROM workstation_master_log")
        final_count = cursor.fetchone()[0]
        
        print(f"🗑️ Deleted {deleted_count:,} records")
        print(f"📊 Final record count: {final_count:,}")
        print(f"✅ workstation_master_log table wiped!")
        
        return deleted_count
        
    except Exception as e:
        print(f"❌ Error wiping workstation_master_log: {e}")
        conn.rollback()
        return 0
    finally:
        cursor.close()
        conn.close()

def wipe_testboard_master_log():
    """Wipe the testboard_master_log table"""
    conn = connect_to_db()
    cursor = conn.cursor()
    
    try:
        print("🧹 Wiping testboard_master_log table...")
        
        # Get initial count
        cursor.execute("SELECT COUNT(*) FROM testboard_master_log")
        initial_count = cursor.fetchone()[0]
        print(f"📊 Initial record count: {initial_count:,}")
        
        # Delete all records
        cursor.execute("DELETE FROM testboard_master_log")
        deleted_count = cursor.rowcount
        conn.commit()
        
        # Get final count
        cursor.execute("SELECT COUNT(*) FROM testboard_master_log")
        final_count = cursor.fetchone()[0]
        
        print(f"🗑️ Deleted {deleted_count:,} records")
        print(f"📊 Final record count: {final_count:,}")
        print(f"✅ testboard_master_log table wiped!")
        
        return deleted_count
        
    except Exception as e:
        print(f"❌ Error wiping testboard_master_log: {e}")
        conn.rollback()
        return 0
    finally:
        cursor.close()
        conn.close()

def main():
    """Main function"""
    print("🚀 Table Wipe Script - ETL_V2")
    print("Wiping all data tables clean")
    print("=" * 60)
    
    start_time = time.time()
    
    # Wipe all tables
    master_count = wipe_master_records()
    workstation_count = wipe_workstation_master_log()
    testboard_count = wipe_testboard_master_log()
    
    total_deleted = master_count + workstation_count + testboard_count
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"\n" + "=" * 60)
    print("🗑️ WIPE COMPLETE!")
    print("=" * 60)
    print(f"   master_records deleted: {master_count:,}")
    print(f"   workstation_master_log deleted: {workstation_count:,}")
    print(f"   testboard_master_log deleted: {testboard_count:,}")
    print(f"   Total deleted: {total_deleted:,}")
    print(f"   Duration: {duration:.2f} seconds")
    
    if total_deleted > 0:
        print(f"\n✅ All tables wiped clean!")
        print(f"   Database: fox_db")
        print(f"   User: gpu_user")
        print(f"   Host: localhost")
        print(f"   Port: 5432")
    else:
        print(f"\n✅ Tables were already empty!")

if __name__ == "__main__":
    main() 