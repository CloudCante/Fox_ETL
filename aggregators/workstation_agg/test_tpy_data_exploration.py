#!/usr/bin/env python3
import psycopg2
from datetime import datetime, timedelta

DB_CONFIG = {
    'host': 'localhost',
    'database': 'fox_db',
    'user': 'gpu_user',
    'password': '',
    'port': '5432'
}

def explore_service_flows():
    """Explore what service flows exist in the data"""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            # Get all unique service flows
            cur.execute("""
                SELECT DISTINCT service_flow 
                FROM workstation_master_log 
                WHERE service_flow IS NOT NULL
                ORDER BY service_flow;
            """)
            
            service_flows = [row[0] for row in cur.fetchall()]
            print("🔍 Service Flows Found:")
            for flow in service_flows:
                print(f"  - {flow}")
            
            return service_flows
    finally:
        conn.close()

def explore_workstations_by_service_flow():
    """See which workstations are in each service flow"""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    service_flow,
                    workstation_name,
                    COUNT(*) as record_count
                FROM workstation_master_log 
                WHERE service_flow IS NOT NULL
                GROUP BY service_flow, workstation_name
                ORDER BY service_flow, record_count DESC;
            """)
            
            results = cur.fetchall()
            print("\n📊 Workstations by Service Flow:")
            current_flow = None
            for flow, station, count in results:
                if flow != current_flow:
                    print(f"\n  {flow}:")
                    current_flow = flow
                print(f"    {station}: {count:,} records")
    finally:
        conn.close()

def test_filtered_data_for_date(target_date):
    """Test filtering and aggregation for a specific date"""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            start_date = target_date
            end_date = target_date + timedelta(days=1)
            
            print(f"\n📅 Testing data for {target_date.strftime('%Y-%m-%d')}")
            print("=" * 50)
            
            # Test 1: Count records by service flow
            cur.execute("""
                SELECT 
                    service_flow,
                    COUNT(*) as record_count
                FROM workstation_master_log 
                WHERE history_station_end_time >= %s 
                    AND history_station_end_time < %s
                    AND service_flow IS NOT NULL
                GROUP BY service_flow
                ORDER BY record_count DESC;
            """, (start_date, end_date))
            
            service_flow_counts = cur.fetchall()
            print("📊 Records by Service Flow:")
            for flow, count in service_flow_counts:
                print(f"  {flow}: {count:,} records")
            
            # Test 2: Count records after filtering out NC Sort and RO
            cur.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT sn) as unique_parts,
                    COUNT(DISTINCT workstation_name) as unique_stations
                FROM workstation_master_log 
                WHERE history_station_end_time >= %s 
                    AND history_station_end_time < %s
                    AND service_flow NOT IN ('NC Sort', 'RO')
                    AND service_flow IS NOT NULL;
            """, (start_date, end_date))
            
            filtered_stats = cur.fetchone()
            print(f"\n✅ After filtering (excluding NC Sort and RO):")
            print(f"  Total records: {filtered_stats[0]:,}")
            print(f"  Unique parts: {filtered_stats[1]:,}")
            print(f"  Unique stations: {filtered_stats[2]:,}")
            
            # Test 3: Show stations and their throughput yields
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
                GROUP BY workstation_name, model
                HAVING COUNT(*) >= 5  -- Only show stations with meaningful volume
                ORDER BY total_parts DESC
                LIMIT 20;
            """, (start_date, end_date))
            
            station_results = cur.fetchall()
            print(f"\n🏭 Top Stations (filtered data):")
            for station, model, total, passed, failed in station_results:
                throughput_yield = (passed / total * 100) if total > 0 else 0
                print(f"  {station} ({model}): {passed}/{total} = {throughput_yield:.1f}%")
            
            # Test 4: Check for models
            cur.execute("""
                SELECT 
                    model,
                    COUNT(*) as record_count
                FROM workstation_master_log 
                WHERE history_station_end_time >= %s 
                    AND history_station_end_time < %s
                    AND service_flow NOT IN ('NC Sort', 'RO')
                    AND service_flow IS NOT NULL
                GROUP BY model
                ORDER BY record_count DESC;
            """, (start_date, end_date))
            
            model_counts = cur.fetchall()
            print(f"\n🚗 Models (filtered data):")
            for model, count in model_counts:
                print(f"  {model}: {count:,} records")
                
    finally:
        conn.close()

def test_tpy_calculation_logic():
    """Test the TPY calculation logic with sample data"""
    print(f"\n🧮 Testing TPY Calculation Logic:")
    print("=" * 40)
    
    # Sample data structure we'll be working with
    sample_data = {
        "Tesla SXM4": {
            "VI2": {"totalParts": 567, "passedParts": 547, "throughputYield": 96.47},
            "ASSY2": {"totalParts": 440, "passedParts": 440, "throughputYield": 100.0},
            "FI": {"totalParts": 438, "passedParts": 438, "throughputYield": 100.0},
            "FQC": {"totalParts": 444, "passedParts": 377, "throughputYield": 84.91}
        },
        "Tesla SXM5": {
            "BBD": {"totalParts": 180, "passedParts": 180, "throughputYield": 100.0},
            "ASSY2": {"totalParts": 90, "passedParts": 90, "throughputYield": 100.0},
            "FI": {"totalParts": 90, "passedParts": 90, "throughputYield": 100.0},
            "FQC": {"totalParts": 116, "passedParts": 114, "throughputYield": 98.28}
        }
    }
    
    # Hardcoded TPY calculation
    print("📈 Hardcoded TPY (4-station formula):")
    
    # SXM4: VI2 × ASSY2 × FI × FQC
    sxm4_stations = ["VI2", "ASSY2", "FI", "FQC"]
    sxm4_tpy = 1.0
    for station in sxm4_stations:
        if station in sample_data["Tesla SXM4"]:
            yield_pct = sample_data["Tesla SXM4"][station]["throughputYield"]
            sxm4_tpy *= (yield_pct / 100.0)
            print(f"  SXM4 {station}: {yield_pct:.2f}%")
    print(f"  🎯 SXM4 TPY: {sxm4_tpy * 100:.2f}%")
    
    # SXM5: BBD × ASSY2 × FI × FQC
    sxm5_stations = ["BBD", "ASSY2", "FI", "FQC"]
    sxm5_tpy = 1.0
    for station in sxm5_stations:
        if station in sample_data["Tesla SXM5"]:
            yield_pct = sample_data["Tesla SXM5"][station]["throughputYield"]
            sxm5_tpy *= (yield_pct / 100.0)
            print(f"  SXM5 {station}: {yield_pct:.2f}%")
    print(f"  🎯 SXM5 TPY: {sxm5_tpy * 100:.2f}%")
    
    # Dynamic TPY calculation
    print(f"\n🚀 Dynamic TPY (all-stations per model):")
    
    # SXM4 dynamic (all stations)
    sxm4_all_stations = sample_data["Tesla SXM4"]
    sxm4_dynamic_tpy = 1.0
    for station, data in sxm4_all_stations.items():
        sxm4_dynamic_tpy *= (data["throughputYield"] / 100.0)
    print(f"  SXM4 Dynamic TPY: {sxm4_dynamic_tpy * 100:.2f}% (across {len(sxm4_all_stations)} stations)")
    
    # SXM5 dynamic (all stations)
    sxm5_all_stations = sample_data["Tesla SXM5"]
    sxm5_dynamic_tpy = 1.0
    for station, data in sxm5_all_stations.items():
        sxm5_dynamic_tpy *= (data["throughputYield"] / 100.0)
    print(f"  SXM5 Dynamic TPY: {sxm5_dynamic_tpy * 100:.2f}% (across {len(sxm5_all_stations)} stations)")

def main():
    print("🔍 TPY Data Exploration Script")
    print("=" * 50)
    
    # Step 1: Explore service flows
    service_flows = explore_service_flows()
    
    # Step 2: Explore workstations by service flow
    explore_workstations_by_service_flow()
    
    # Step 3: Test filtered data for a recent date
    test_date = datetime.now().date() - timedelta(days=1)  # Yesterday
    test_filtered_data_for_date(test_date)
    
    # Step 4: Test TPY calculation logic
    test_tpy_calculation_logic()
    
    print(f"\n✅ Exploration complete! Ready to build the aggregator.")

if __name__ == "__main__":
    main() 