# ETL V2 Daily Operations Schedule

## 🚀 DAILY OPERATIONS (Run Every Day)

### 1. **Data Ingestion** (Priority: CRITICAL)
```bash
# Upload new workstation data
python upload_workstation_master_log.py

# Upload new testboard data  
python upload_testboard_master_log.py
```
**When**: After new data arrives (typically morning)
**Purpose**: Load fresh production data into master tables

### 2. **Daily Aggregations** (Priority: HIGH)
```bash
# Daily TPY metrics (workstation yield)
cd aggregators/workstation_agg
python aggregate_tpy_daily.py --mode recent

# Daily packing metrics
python aggregate_packing_daily_dedup.py

# Daily sort test metrics  
python aggregate_sort_test_weekly_dedup.py
```
**When**: After data ingestion (morning/afternoon)
**Purpose**: Calculate daily performance metrics for dashboard

### 3. **Data Quality Checks** (Priority: MEDIUM)
```bash
# Check for duplicates
python cleanup_duplicates.py

# Verify record counts
python check_record_counts.py
```
**When**: After aggregations complete
**Purpose**: Ensure data integrity

## 📅 WEEKLY OPERATIONS (Run Once Per Week)

### 1. **Weekly Aggregations** (Priority: HIGH)
```bash
# Weekly TPY metrics
cd aggregators/workstation_agg
python aggregate_tpy_weekly.py --mode recent

# Weekly packing metrics
python aggregate_packing_weekly_dedup.py

# All-time testboard metrics
cd ../testboard_agg
python aggregate_all_time_dedup.py
```
**When**: End of week (Friday/Saturday)
**Purpose**: Generate weekly summaries for reporting

### 2. **Full Dataset Refresh** (Priority: MEDIUM)
```bash
# Complete daily aggregation for all dates
cd aggregators/workstation_agg
python aggregate_tpy_daily.py --mode all

# Complete weekly aggregation for all weeks
python aggregate_tpy_weekly.py --mode all
```
**When**: Weekend maintenance window
**Purpose**: Ensure complete historical data

## 🔧 ON-DEMAND OPERATIONS

### 1. **Data Cleanup** (As Needed)
```bash
# Remove duplicates
python cleanup_duplicates.py

# Wipe specific tables for re-processing
python wipe_tables.py

# Clean database schema
python cleanup_database_schema.py
```
**When**: When data quality issues detected
**Purpose**: Fix data integrity problems

### 2. **Debugging Tools** (As Needed)
```bash
# Debug data comparison
python debug_comparison.py

# Check database records
python debug_database_records.py

# Debug deduplication
python debug_deduplication.py
```
**When**: When investigating issues
**Purpose**: Troubleshoot data problems

### 3. **Testing & Validation** (As Needed)
```bash
# Test specific dates
cd aggregators/workstation_agg
python test_tpy_specific_date.py

# Verify data availability
python verify_data_availability.py

# Check table structure
python check_table_structure.py
```
**When**: When validating new features or data
**Purpose**: Ensure system reliability

## 📊 MONITORING CHECKLIST

### Daily Checks:
- [ ] New data uploaded successfully
- [ ] Daily aggregations completed without errors
- [ ] Dashboard metrics updated
- [ ] No duplicate records created
- [ ] Record counts reasonable

### Weekly Checks:
- [ ] Weekly aggregations completed
- [ ] Historical data complete
- [ ] Performance metrics calculated
- [ ] Data quality maintained

## 🚨 ERROR HANDLING

### If Daily Aggregations Fail:
1. Check data quality: `python check_record_counts.py`
2. Clean duplicates: `python cleanup_duplicates.py`
3. Re-run aggregations: `python aggregate_tpy_daily.py --mode recent`

### If Weekly Aggregations Fail:
1. Verify daily data: `python verify_data_availability.py`
2. Re-run daily for all dates: `python aggregate_tpy_daily.py --mode all`
3. Re-run weekly: `python aggregate_tpy_weekly.py --mode all`

## 📈 PERFORMANCE METRICS TO TRACK

### Daily Metrics:
- Records processed per day
- Aggregation execution time
- Error rates
- Data quality scores

### Weekly Metrics:
- Total records in each table
- Aggregation completion rates
- System uptime
- Dashboard response times

## 🔄 AUTOMATION RECOMMENDATIONS

### Cron Jobs to Set Up:
```bash
# Daily at 6 AM - Data ingestion
0 6 * * * cd /path/to/ETL_V2 && python upload_workstation_master_log.py
0 6 * * * cd /path/to/ETL_V2 && python upload_testboard_master_log.py

# Daily at 7 AM - Daily aggregations
0 7 * * * cd /path/to/ETL_V2/aggregators/workstation_agg && python aggregate_tpy_daily.py --mode recent
0 7 * * * cd /path/to/ETL_V2/aggregators/workstation_agg && python aggregate_packing_daily_dedup.py

# Weekly on Saturday at 2 AM - Weekly aggregations
0 2 * * 6 cd /path/to/ETL_V2/aggregators/workstation_agg && python aggregate_tpy_weekly.py --mode recent
0 2 * * 6 cd /path/to/ETL_V2/aggregators/workstation_agg && python aggregate_packing_weekly_dedup.py
```

### Monitoring Script:
Consider creating a daily monitoring script that:
- Runs all daily operations
- Logs success/failure
- Sends alerts on errors
- Generates daily summary report

## 📝 OPERATIONAL NOTES

### Critical Path:
1. **Data Ingestion** → **Daily Aggregations** → **Dashboard Updates**
2. Any failure in this chain affects dashboard availability

### Backup Strategy:
- Keep raw data backups before processing
- Maintain aggregation result backups
- Document all manual interventions

### Scaling Considerations:
- Monitor database size growth
- Track aggregation execution times
- Plan for data retention policies
- Consider partitioning for large datasets 