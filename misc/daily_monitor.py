#!/usr/bin/env python3
"""
Daily ETL Operations Monitor
Runs essential daily operations and provides status reporting
"""

import subprocess
import sys
import os
from datetime import datetime
import psycopg2
import argparse

DB_CONFIG = {
    'host': 'localhost',
    'database': 'fox_db',
    'user': 'gpu_user',
    'password': '',
    'port': '5432'
}

def log_message(message, level="INFO"):
    """Log a message with timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {level}: {message}")

def run_command(command, description):
    """Run a command and return success status"""
    log_message(f"Starting: {description}")
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            log_message(f"✅ Success: {description}")
            return True
        else:
            log_message(f"❌ Failed: {description}", "ERROR")
            log_message(f"Error output: {result.stderr}", "ERROR")
            return False
    except Exception as e:
        log_message(f"❌ Exception in {description}: {str(e)}", "ERROR")
        return False

def check_database_health():
    """Check basic database connectivity and table counts"""
    log_message("Checking database health...")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            # Check master tables
            cur.execute("SELECT COUNT(*) FROM workstation_master_log")
            workstation_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM testboard_master_log")
            testboard_count = cur.fetchone()[0]
            
            # Check aggregation tables
            cur.execute("SELECT COUNT(*) FROM daily_tpy_metrics")
            daily_tpy_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM weekly_tpy_metrics")
            weekly_tpy_count = cur.fetchone()[0]
            
            log_message(f"📊 Database Status:")
            log_message(f"  - Workstation records: {workstation_count:,}")
            log_message(f"  - Testboard records: {testboard_count:,}")
            log_message(f"  - Daily TPY records: {daily_tpy_count:,}")
            log_message(f"  - Weekly TPY records: {weekly_tpy_count:,}")
            
            return True
    except Exception as e:
        log_message(f"❌ Database health check failed: {str(e)}", "ERROR")
        return False
    finally:
        if 'conn' in locals():
            conn.close()

def run_daily_operations():
    """Run essential daily operations"""
    log_message("🚀 Starting daily ETL operations...")
    
    operations = [
        # Data ingestion
        ("python upload_workstation_master_log.py", "Upload workstation data"),
        ("python upload_testboard_master_log.py", "Upload testboard data"),
        
        # Daily aggregations
        ("cd aggregators/workstation_agg && python aggregate_tpy_daily.py --mode recent", "Daily TPY aggregation"),
        ("cd aggregators/workstation_agg && python aggregate_packing_daily_dedup.py", "Daily packing aggregation"),
        
        # Data quality checks
        ("python cleanup_duplicates.py", "Clean duplicates"),
        ("python check_record_counts.py", "Check record counts"),
    ]
    
    success_count = 0
    total_operations = len(operations)
    
    for command, description in operations:
        if run_command(command, description):
            success_count += 1
    
    log_message(f"📈 Daily operations complete: {success_count}/{total_operations} successful")
    return success_count == total_operations

def run_weekly_operations():
    """Run weekly operations (typically run on weekends)"""
    log_message("📅 Starting weekly ETL operations...")
    
    operations = [
        ("cd aggregators/workstation_agg && python aggregate_tpy_weekly.py --mode recent", "Weekly TPY aggregation"),
        ("cd aggregators/workstation_agg && python aggregate_packing_weekly_dedup.py", "Weekly packing aggregation"),
        ("cd aggregators/testboard_agg && python aggregate_all_time_dedup.py", "Testboard all-time aggregation"),
    ]
    
    success_count = 0
    total_operations = len(operations)
    
    for command, description in operations:
        if run_command(command, description):
            success_count += 1
    
    log_message(f"📈 Weekly operations complete: {success_count}/{total_operations} successful")
    return success_count == total_operations

def generate_daily_report():
    """Generate a summary report of daily operations"""
    log_message("📋 Generating daily report...")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            # Get today's date
            today = datetime.now().date()
            
            # Check today's aggregations
            cur.execute("""
                SELECT COUNT(*) FROM daily_tpy_metrics 
                WHERE date_id = %s
            """, (today,))
            today_daily_count = cur.fetchone()[0]
            
            # Check recent weekly data
            cur.execute("""
                SELECT COUNT(*) FROM weekly_tpy_metrics 
                WHERE created_at >= %s
            """, (today,))
            recent_weekly_count = cur.fetchone()[0]
            
            # Get latest data timestamps
            cur.execute("""
                SELECT MAX(history_station_end_time) FROM workstation_master_log
            """)
            latest_workstation = cur.fetchone()[0]
            
            cur.execute("""
                SELECT MAX(history_station_end_time) FROM testboard_master_log
            """)
            latest_testboard = cur.fetchone()[0]
            
            log_message(f"📊 Daily Report Summary:")
            log_message(f"  - Date: {today}")
            log_message(f"  - Today's daily aggregations: {today_daily_count}")
            log_message(f"  - Recent weekly aggregations: {recent_weekly_count}")
            log_message(f"  - Latest workstation data: {latest_workstation}")
            log_message(f"  - Latest testboard data: {latest_testboard}")
            
    except Exception as e:
        log_message(f"❌ Failed to generate report: {str(e)}", "ERROR")
    finally:
        if 'conn' in locals():
            conn.close()

def main():
    parser = argparse.ArgumentParser(description="ETL Daily Operations Monitor")
    parser.add_argument('--mode', choices=['daily', 'weekly', 'health', 'report'], 
                       default='daily', help="Operation mode")
    parser.add_argument('--check-only', action='store_true', 
                       help="Only check status, don't run operations")
    
    args = parser.parse_args()
    
    log_message("🔧 ETL Daily Monitor Starting...")
    
    # Always check database health first
    if not check_database_health():
        log_message("❌ Database health check failed. Exiting.", "ERROR")
        sys.exit(1)
    
    if args.check_only:
        log_message("🔍 Check-only mode - no operations will be run")
        generate_daily_report()
        return
    
    if args.mode == 'daily':
        success = run_daily_operations()
        generate_daily_report()
        sys.exit(0 if success else 1)
    
    elif args.mode == 'weekly':
        success = run_weekly_operations()
        generate_daily_report()
        sys.exit(0 if success else 1)
    
    elif args.mode == 'health':
        log_message("✅ Health check completed")
    
    elif args.mode == 'report':
        generate_daily_report()

if __name__ == "__main__":
    main() 