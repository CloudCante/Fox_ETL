#!/usr/bin/env python3
import psycopg2
import json
from datetime import datetime, timedelta
import argparse

DB_CONFIG = {
    'host': 'localhost',
    'database': 'fox_db',
    'user': 'gpu_user',
    'password': '',
    'port': '5432'
}

def get_iso_week_id(date_obj):
    """Convert date to ISO week format: 2025-W23"""
    year, week, _ = date_obj.isocalendar()
    return f"{year}-W{week:02d}"

def get_week_date_range(week_id):
    """Get start and end dates for an ISO week"""
    year, week = week_id.split('-W')
    year = int(year)
    week = int(week)
    
    # ISO week 1 is the first week with Thursday in the new year
    jan4 = datetime(year, 1, 4)
    week_start = jan4 - timedelta(days=jan4.weekday()) + timedelta(weeks=week-1)
    week_end = week_start + timedelta(days=6)
    
    return week_start.date(), week_end.date()

def calculate_weekly_first_pass_yield_from_raw(week_start, week_end):
    """Calculate WEEKLY first pass yield using raw data from workstation_master_log"""
    print(f"🎯 Calculating WEEKLY First Pass Yield from raw data...")
    print(f"  Week range: {week_start.strftime('%Y-%m-%d')} to {week_end.strftime('%Y-%m-%d')}")
    
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            # Analyze each part's weekly performance using raw data
            cur.execute("""
                WITH part_analysis AS (
                    SELECT 
                        sn,
                        model,
                        COUNT(CASE WHEN workstation_name = 'PACKING' THEN 1 END) as reached_packing,
                        COUNT(CASE WHEN history_station_passing_status != 'Pass' THEN 1 END) as failure_count
                    FROM workstation_master_log
                    WHERE history_station_end_time >= %s 
                        AND history_station_end_time < %s
                        AND service_flow NOT IN ('NC Sort', 'RO')
                        AND service_flow IS NOT NULL
                    GROUP BY sn, model
                )
                SELECT 
                    COUNT(*) as parts_started,
                    COUNT(CASE WHEN reached_packing > 0 AND failure_count = 0 THEN 1 END) as first_pass_success,
                    COUNT(CASE WHEN reached_packing > 0 THEN 1 END) as parts_completed,
                    COUNT(CASE WHEN failure_count > 0 THEN 1 END) as parts_failed,
                    COUNT(CASE WHEN reached_packing = 0 AND failure_count = 0 THEN 1 END) as parts_stuck_in_limbo
                FROM part_analysis;
            """, (week_start, week_end + timedelta(days=1)))
            
            result = cur.fetchone()
            
            if result and result[0] > 0:
                parts_started, first_pass_success, parts_completed, parts_failed, parts_stuck = result
                
                # Traditional calculation
                traditional_fpy = (first_pass_success / parts_started * 100) if parts_started > 0 else 0
                
                # Completed-only calculation (excluding stuck parts)
                active_parts = parts_completed + parts_failed
                completed_only_fpy = (first_pass_success / active_parts * 100) if active_parts > 0 else 0
                
                print(f"  📊 TRADITIONAL FPY: {first_pass_success}/{parts_started} = {traditional_fpy:.2f}%")
                print(f"  🆕 COMPLETED-ONLY FPY: {first_pass_success}/{active_parts} = {completed_only_fpy:.2f}%")
                print(f"  📈 Parts breakdown:")
                print(f"    ✅ Completed: {parts_completed}")
                print(f"    ❌ Failed: {parts_failed}")
                print(f"    ⏸️  Stuck in limbo: {parts_stuck}")
                print(f"    📊 Total: {parts_started}")
                
                return {
                    "traditional": {
                        "partsStarted": parts_started,
                        "firstPassSuccess": first_pass_success,
                        "firstPassYield": round(traditional_fpy, 2)
                    },
                    "completedOnly": {
                        "activeParts": active_parts,
                        "firstPassSuccess": first_pass_success,
                        "firstPassYield": round(completed_only_fpy, 2)
                    },
                    "breakdown": {
                        "partsCompleted": parts_completed,
                        "partsFailed": parts_failed,
                        "partsStuckInLimbo": parts_stuck,
                        "totalParts": parts_started
                    }
                }
            else:
                print(f"  📈 No data found for this week")
                return {
                    "traditional": {"partsStarted": 0, "firstPassSuccess": 0, "firstPassYield": 0},
                    "completedOnly": {"activeParts": 0, "firstPassSuccess": 0, "firstPassYield": 0},
                    "breakdown": {"partsCompleted": 0, "partsFailed": 0, "partsStuckInLimbo": 0, "totalParts": 0}
                }
    finally:
        conn.close() 

def calculate_model_specific_throughput_yields(week_start, week_end):
    """Calculate MODEL-SPECIFIC throughput yields from raw data"""
    print(f"🔧 Calculating MODEL-SPECIFIC Throughput Yields...")
    
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    model,
                    workstation_name,
                    COUNT(*) as total_parts,
                    COUNT(CASE WHEN history_station_passing_status = 'Pass' THEN 1 END) as passed_parts,
                    COUNT(CASE WHEN history_station_passing_status != 'Pass' THEN 1 END) as failed_parts
                FROM workstation_master_log 
                WHERE history_station_end_time >= %s 
                    AND history_station_end_time < %s
                    AND service_flow NOT IN ('NC Sort', 'RO')
                    AND service_flow IS NOT NULL
                    AND model IN ('Tesla SXM4', 'Tesla SXM5')
                GROUP BY model, workstation_name
                HAVING COUNT(*) >= 1
                ORDER BY model, total_parts DESC;
            """, (week_start, week_end + timedelta(days=1)))
            
            results = cur.fetchall()
            
            # Organize by model
            model_specific_yields = {
                "Tesla SXM4": {},
                "Tesla SXM5": {},
                "overall": {}
            }
            
            # Aggregate for overall station metrics
            overall_aggregates = {}
            
            print(f"  📊 Model-Specific Station Performance:")
            
            for model, station, total, passed, failed in results:
                throughput_yield = (passed / total * 100) if total > 0 else 0
                
                # Store model-specific data
                if model in model_specific_yields:
                    model_specific_yields[model][station] = {
                        "totalParts": total,
                        "passedParts": passed,
                        "failedParts": failed,
                        "throughputYield": round(throughput_yield, 2)
                    }
                    print(f"    {model} {station}: {passed}/{total} = {throughput_yield:.1f}%")
                
                # Aggregate for overall metrics
                if station not in overall_aggregates:
                    overall_aggregates[station] = {'totalParts': 0, 'passedParts': 0, 'failedParts': 0}
                
                overall_aggregates[station]['totalParts'] += total
                overall_aggregates[station]['passedParts'] += passed
                overall_aggregates[station]['failedParts'] += failed
            
            # Calculate overall station metrics
            for station, totals in overall_aggregates.items():
                throughput_yield = (totals['passedParts'] / totals['totalParts'] * 100) if totals['totalParts'] > 0 else 0
                model_specific_yields["overall"][station] = {
                    "totalParts": totals['totalParts'],
                    "passedParts": totals['passedParts'],
                    "failedParts": totals['failedParts'],
                    "throughputYield": round(throughput_yield, 2)
                }
            
            return model_specific_yields
    finally:
        conn.close()

def calculate_hardcoded_tpy(model_yields):
    """Calculate hardcoded 4-station TPY"""
    print(f"🧮 Calculating HARDCODED TPY (4-station formula)...")
    
    hardcoded_tpy = {
        "SXM4": {"stations": {}, "tpy": None},
        "SXM5": {"stations": {}, "tpy": None}
    }
    
    # SXM4 formula: VI2 × ASSY2 × FI × FQC
    sxm4_stations = ["VI2", "ASSY2", "FI", "FQC"]
    sxm4_values = []
    
    for station in sxm4_stations:
        if "Tesla SXM4" in model_yields and station in model_yields["Tesla SXM4"]:
            yield_pct = model_yields["Tesla SXM4"][station]["throughputYield"]
            hardcoded_tpy["SXM4"]["stations"][station] = yield_pct
            sxm4_values.append(yield_pct / 100.0)
            print(f"  📊 SXM4 {station}: {yield_pct:.2f}%")
        else:
            print(f"  ⚠️  SXM4 {station}: NOT FOUND")
    
    # Calculate SXM4 TPY
    if len(sxm4_values) == 4:
        tpy_value = 1.0
        for val in sxm4_values:
            tpy_value *= val
        hardcoded_tpy["SXM4"]["tpy"] = round(tpy_value * 100, 2)
        print(f"  🎯 SXM4 Hardcoded TPY: {hardcoded_tpy['SXM4']['tpy']:.2f}%")
    
    # SXM5 formula: BBD × ASSY2 × FI × FQC
    sxm5_stations = ["BBD", "ASSY2", "FI", "FQC"]
    sxm5_values = []
    
    for station in sxm5_stations:
        if "Tesla SXM5" in model_yields and station in model_yields["Tesla SXM5"]:
            yield_pct = model_yields["Tesla SXM5"][station]["throughputYield"]
            hardcoded_tpy["SXM5"]["stations"][station] = yield_pct
            sxm5_values.append(yield_pct / 100.0)
            print(f"  📊 SXM5 {station}: {yield_pct:.2f}%")
        else:
            print(f"  ⚠️  SXM5 {station}: NOT FOUND")
    
    # Calculate SXM5 TPY
    if len(sxm5_values) == 4:
        tpy_value = 1.0
        for val in sxm5_values:
            tpy_value *= val
        hardcoded_tpy["SXM5"]["tpy"] = round(tpy_value * 100, 2)
        print(f"  🎯 SXM5 Hardcoded TPY: {hardcoded_tpy['SXM5']['tpy']:.2f}%")
    
    return hardcoded_tpy 

def calculate_dynamic_tpy(model_yields):
    """Calculate DYNAMIC all-stations TPY per model"""
    print(f"🆕 Calculating DYNAMIC TPY (all-stations per model)...")
    
    dynamic_tpy = {
        "SXM4": {"stations": {}, "tpy": None, "stationCount": 0},
        "SXM5": {"stations": {}, "tpy": None, "stationCount": 0}
    }
    
    # SXM4 dynamic TPY (all stations Tesla SXM4 parts went through)
    if "Tesla SXM4" in model_yields:
        sxm4_stations = model_yields["Tesla SXM4"]
        dynamic_tpy["SXM4"]["stations"] = {station: data["throughputYield"] for station, data in sxm4_stations.items()}
        dynamic_tpy["SXM4"]["stationCount"] = len(sxm4_stations)
        
        # Multiply all SXM4 stations together
        tpy_value = 1.0
        for station, yield_pct in dynamic_tpy["SXM4"]["stations"].items():
            tpy_value *= (yield_pct / 100.0)
        
        dynamic_tpy["SXM4"]["tpy"] = round(tpy_value * 100, 2)
        print(f"  🚀 SXM4 Dynamic TPY: {dynamic_tpy['SXM4']['tpy']:.2f}% (across {dynamic_tpy['SXM4']['stationCount']} stations)")
    
    # SXM5 dynamic TPY (all stations Tesla SXM5 parts went through)
    if "Tesla SXM5" in model_yields:
        sxm5_stations = model_yields["Tesla SXM5"]
        dynamic_tpy["SXM5"]["stations"] = {station: data["throughputYield"] for station, data in sxm5_stations.items()}
        dynamic_tpy["SXM5"]["stationCount"] = len(sxm5_stations)
        
        # Multiply all SXM5 stations together
        tpy_value = 1.0
        for station, yield_pct in dynamic_tpy["SXM5"]["stations"].items():
            tpy_value *= (yield_pct / 100.0)
        
        dynamic_tpy["SXM5"]["tpy"] = round(tpy_value * 100, 2)
        print(f"  🚀 SXM5 Dynamic TPY: {dynamic_tpy['SXM5']['tpy']:.2f}% (across {dynamic_tpy['SXM5']['stationCount']} stations)")
    
    return dynamic_tpy

def aggregate_weekly_tpy_for_week(week_id):
    """Aggregate weekly TPY metrics for a specific week"""
    print(f"\n📊 AGGREGATING WEEKLY TPY FOR: {week_id}")
    print("=" * 60)
    
    week_start, week_end = get_week_date_range(week_id)
    
    # 1. Calculate weekly first pass yield from raw data
    weekly_first_pass_yield = calculate_weekly_first_pass_yield_from_raw(week_start, week_end)
    
    # 2. Calculate model-specific throughput yields from raw data
    model_specific_yields = calculate_model_specific_throughput_yields(week_start, week_end)
    
    # 3. Calculate hardcoded TPY
    hardcoded_tpy = calculate_hardcoded_tpy(model_specific_yields)
    
    # 4. Calculate dynamic TPY
    dynamic_tpy = calculate_dynamic_tpy(model_specific_yields)
    
    # 5. Calculate overall yield from daily data
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            # Get daily data for this week
            cur.execute("""
                SELECT 
                    SUM(total_parts) as total_parts,
                    SUM(passed_parts) as passed_parts
                FROM daily_tpy_metrics_historical 
                WHERE date_id >= %s AND date_id <= %s
            """, (week_start, week_end))
            
            daily_result = cur.fetchone()
            total_parts_overall = daily_result[0] if daily_result[0] else 0
            total_passed_parts = daily_result[1] if daily_result[1] else 0
            overall_yield = (total_passed_parts / total_parts_overall * 100) if total_parts_overall > 0 else 0
            
            # Use overall station metrics for backward compatibility
            weekly_station_metrics = model_specific_yields["overall"]
            avg_throughput_yield = round(sum(s["throughputYield"] for s in weekly_station_metrics.values()) / len(weekly_station_metrics), 2) if weekly_station_metrics else 0
            
            # Find best and worst stations
            best_station = None
            worst_station = None
            if weekly_station_metrics:
                best_station = max(weekly_station_metrics.items(), key=lambda x: x[1]["throughputYield"])
                worst_station = min(weekly_station_metrics.items(), key=lambda x: x[1]["throughputYield"])
            
            # 6. Insert weekly TPY metrics
            cur.execute("""
                INSERT INTO weekly_tpy_metrics_historical (
                    week_id, week_start, week_end, days_in_week,
                    weekly_first_pass_yield_traditional_parts_started,
                    weekly_first_pass_yield_traditional_first_pass_success,
                    weekly_first_pass_yield_traditional_first_pass_yield,
                    weekly_first_pass_yield_completed_only_active_parts,
                    weekly_first_pass_yield_completed_only_first_pass_success,
                    weekly_first_pass_yield_completed_only_first_pass_yield,
                    weekly_first_pass_yield_breakdown_parts_completed,
                    weekly_first_pass_yield_breakdown_parts_failed,
                    weekly_first_pass_yield_breakdown_parts_stuck_in_limbo,
                    weekly_first_pass_yield_breakdown_total_parts,
                    weekly_overall_yield_total_parts,
                    weekly_overall_yield_completed_parts,
                    weekly_overall_yield_overall_yield,
                    weekly_throughput_yield_station_metrics,
                    weekly_throughput_yield_average_yield,
                    weekly_throughput_yield_model_specific,
                    weekly_tpy_hardcoded_sxm4_stations,
                    weekly_tpy_hardcoded_sxm4_tpy,
                    weekly_tpy_hardcoded_sxm5_stations,
                    weekly_tpy_hardcoded_sxm5_tpy,
                    weekly_tpy_dynamic_sxm4_stations,
                    weekly_tpy_dynamic_sxm4_tpy,
                    weekly_tpy_dynamic_sxm4_station_count,
                    weekly_tpy_dynamic_sxm5_stations,
                    weekly_tpy_dynamic_sxm5_tpy,
                    weekly_tpy_dynamic_sxm5_station_count,
                    total_stations,
                    best_station_name,
                    best_station_yield,
                    worst_station_name,
                    worst_station_yield
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) ON CONFLICT (week_id) DO UPDATE SET
                    week_start = EXCLUDED.week_start,
                    week_end = EXCLUDED.week_end,
                    days_in_week = EXCLUDED.days_in_week,
                    weekly_first_pass_yield_traditional_parts_started = EXCLUDED.weekly_first_pass_yield_traditional_parts_started,
                    weekly_first_pass_yield_traditional_first_pass_success = EXCLUDED.weekly_first_pass_yield_traditional_first_pass_success,
                    weekly_first_pass_yield_traditional_first_pass_yield = EXCLUDED.weekly_first_pass_yield_traditional_first_pass_yield,
                    weekly_first_pass_yield_completed_only_active_parts = EXCLUDED.weekly_first_pass_yield_completed_only_active_parts,
                    weekly_first_pass_yield_completed_only_first_pass_success = EXCLUDED.weekly_first_pass_yield_completed_only_first_pass_success,
                    weekly_first_pass_yield_completed_only_first_pass_yield = EXCLUDED.weekly_first_pass_yield_completed_only_first_pass_yield,
                    weekly_first_pass_yield_breakdown_parts_completed = EXCLUDED.weekly_first_pass_yield_breakdown_parts_completed,
                    weekly_first_pass_yield_breakdown_parts_failed = EXCLUDED.weekly_first_pass_yield_breakdown_parts_failed,
                    weekly_first_pass_yield_breakdown_parts_stuck_in_limbo = EXCLUDED.weekly_first_pass_yield_breakdown_parts_stuck_in_limbo,
                    weekly_first_pass_yield_breakdown_total_parts = EXCLUDED.weekly_first_pass_yield_breakdown_total_parts,
                    weekly_overall_yield_total_parts = EXCLUDED.weekly_overall_yield_total_parts,
                    weekly_overall_yield_completed_parts = EXCLUDED.weekly_overall_yield_completed_parts,
                    weekly_overall_yield_overall_yield = EXCLUDED.weekly_overall_yield_overall_yield,
                    weekly_throughput_yield_station_metrics = EXCLUDED.weekly_throughput_yield_station_metrics,
                    weekly_throughput_yield_average_yield = EXCLUDED.weekly_throughput_yield_average_yield,
                    weekly_throughput_yield_model_specific = EXCLUDED.weekly_throughput_yield_model_specific,
                    weekly_tpy_hardcoded_sxm4_stations = EXCLUDED.weekly_tpy_hardcoded_sxm4_stations,
                    weekly_tpy_hardcoded_sxm4_tpy = EXCLUDED.weekly_tpy_hardcoded_sxm4_tpy,
                    weekly_tpy_hardcoded_sxm5_stations = EXCLUDED.weekly_tpy_hardcoded_sxm5_stations,
                    weekly_tpy_hardcoded_sxm5_tpy = EXCLUDED.weekly_tpy_hardcoded_sxm5_tpy,
                    weekly_tpy_dynamic_sxm4_stations = EXCLUDED.weekly_tpy_dynamic_sxm4_stations,
                    weekly_tpy_dynamic_sxm4_tpy = EXCLUDED.weekly_tpy_dynamic_sxm4_tpy,
                    weekly_tpy_dynamic_sxm4_station_count = EXCLUDED.weekly_tpy_dynamic_sxm4_station_count,
                    weekly_tpy_dynamic_sxm5_stations = EXCLUDED.weekly_tpy_dynamic_sxm5_stations,
                    weekly_tpy_dynamic_sxm5_tpy = EXCLUDED.weekly_tpy_dynamic_sxm5_tpy,
                    weekly_tpy_dynamic_sxm5_station_count = EXCLUDED.weekly_tpy_dynamic_sxm5_station_count,
                    total_stations = EXCLUDED.total_stations,
                    best_station_name = EXCLUDED.best_station_name,
                    best_station_yield = EXCLUDED.best_station_yield,
                    worst_station_name = EXCLUDED.worst_station_name,
                    worst_station_yield = EXCLUDED.worst_station_yield,
                    created_at = NOW();
            """, (
                week_id, week_start, week_end, 7,  # days_in_week = 7
                weekly_first_pass_yield["traditional"]["partsStarted"],
                weekly_first_pass_yield["traditional"]["firstPassSuccess"],
                weekly_first_pass_yield["traditional"]["firstPassYield"],
                weekly_first_pass_yield["completedOnly"]["activeParts"],
                weekly_first_pass_yield["completedOnly"]["firstPassSuccess"],
                weekly_first_pass_yield["completedOnly"]["firstPassYield"],
                weekly_first_pass_yield["breakdown"]["partsCompleted"],
                weekly_first_pass_yield["breakdown"]["partsFailed"],
                weekly_first_pass_yield["breakdown"]["partsStuckInLimbo"],
                weekly_first_pass_yield["breakdown"]["totalParts"],
                total_parts_overall,
                total_passed_parts,
                round(overall_yield, 2),
                json.dumps(weekly_station_metrics),
                avg_throughput_yield,
                json.dumps(model_specific_yields),
                json.dumps(hardcoded_tpy["SXM4"]["stations"]),
                hardcoded_tpy["SXM4"]["tpy"],
                json.dumps(hardcoded_tpy["SXM5"]["stations"]),
                hardcoded_tpy["SXM5"]["tpy"],
                json.dumps(dynamic_tpy["SXM4"]["stations"]),
                dynamic_tpy["SXM4"]["tpy"],
                dynamic_tpy["SXM4"]["stationCount"],
                json.dumps(dynamic_tpy["SXM5"]["stations"]),
                dynamic_tpy["SXM5"]["tpy"],
                dynamic_tpy["SXM5"]["stationCount"],
                len(weekly_station_metrics),
                best_station[0] if best_station else None,
                best_station[1]["throughputYield"] if best_station else None,
                worst_station[0] if worst_station else None,
                worst_station[1]["throughputYield"] if worst_station else None
            ))
            
            conn.commit()
            
            print(f"\n✅ Weekly TPY aggregation complete!")
            print(f"  📊 Week: {week_id}")
            print(f"  🎯 Traditional FPY: {weekly_first_pass_yield['traditional']['firstPassYield']:.1f}%")
            print(f"  🆕 Completed-Only FPY: {weekly_first_pass_yield['completedOnly']['firstPassYield']:.1f}%")
            print(f"  📈 SXM4 Hardcoded TPY: {hardcoded_tpy['SXM4']['tpy']:.2f}%")
            print(f"  🚀 SXM4 Dynamic TPY: {dynamic_tpy['SXM4']['tpy']:.2f}%")
            print(f"  📈 SXM5 Hardcoded TPY: {hardcoded_tpy['SXM5']['tpy']:.2f}%")
            print(f"  🚀 SXM5 Dynamic TPY: {dynamic_tpy['SXM5']['tpy']:.2f}%")
            
    finally:
        conn.close() 

def get_all_available_weeks():
    """Get all ISO weeks that have daily data"""
    print("📅 Finding all weeks with daily metrics...")
    
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT week_id
                FROM (
                    SELECT 
                        CONCAT(EXTRACT(YEAR FROM date_id), '-W', LPAD(EXTRACT(WEEK FROM date_id)::text, 2, '0')) as week_id
                    FROM daily_tpy_metrics_historical
                ) weeks
                ORDER BY week_id;
            """)
            
            weeks = [row[0] for row in cur.fetchall()]
            print(f"  Found {len(weeks)} weeks from {weeks[0] if weeks else 'None'} to {weeks[-1] if weeks else 'None'}")
            return weeks
    finally:
        conn.close()

def aggregate_weekly_tpy_metrics_all_time():
    """Aggregate weekly TPY metrics for all historical weeks"""
    print("📊 WEEKLY TPY METRICS ALL-TIME AGGREGATOR")
    print("=" * 50)
    
    # Get all available weeks
    weeks_to_process = get_all_available_weeks()
    if not weeks_to_process:
        print("❌ No valid weeks found")
        return
    
    print(f"\n🔄 Processing ALL {len(weeks_to_process)} historical weeks...")
    
    success_count = 0
    error_count = 0
    
    for i, week_id in enumerate(weeks_to_process, 1):
        try:
            print(f"\n📍 Processing {i}/{len(weeks_to_process)}: {week_id}")
            print("-" * 40)
            
            aggregate_weekly_tpy_for_week(week_id)
            success_count += 1
            
        except Exception as e:
            print(f"  ❌ ERROR processing {week_id}: {str(e)}")
            error_count += 1
    
    # Final summary
    print(f"\n🎉 WEEKLY TPY ALL-TIME AGGREGATION COMPLETE!")
    print(f"  ✅ Successfully processed: {success_count} weeks")
    print(f"  ❌ Errors: {error_count} weeks")
    
    # Show sample results
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM weekly_tpy_metrics_historical")
            total_records = cur.fetchone()[0]
            print(f"  📊 Total records in weekly_tpy_metrics_historical: {total_records}")
            
            if total_records > 0:
                cur.execute("""
                    SELECT week_id, weekly_tpy_hardcoded_sxm4_tpy, weekly_tpy_hardcoded_sxm5_tpy,
                           weekly_tpy_dynamic_sxm4_tpy, weekly_tpy_dynamic_sxm5_tpy
                    FROM weekly_tpy_metrics_historical 
                    ORDER BY week_id DESC 
                    LIMIT 3;
                """)
                sample_results = cur.fetchall()
                print(f"\n📋 SAMPLE RESULTS:")
                for week_id, sxm4_hard, sxm5_hard, sxm4_dyn, sxm5_dyn in sample_results:
                    print(f"  {week_id}: SXM4 Hard {sxm4_hard:.1f}% Dyn {sxm4_dyn:.1f}%, SXM5 Hard {sxm5_hard:.1f}% Dyn {sxm5_dyn:.1f}%")
    finally:
        conn.close()

if __name__ == "__main__":
    aggregate_weekly_tpy_metrics_all_time() 