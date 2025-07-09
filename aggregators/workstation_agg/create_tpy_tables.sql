-- Create daily TPY metrics table
CREATE TABLE IF NOT EXISTS daily_tpy_metrics (
    date_id DATE NOT NULL,
    model VARCHAR(20) NOT NULL,
    workstation_name VARCHAR(50) NOT NULL,
    total_parts INTEGER NOT NULL,
    passed_parts INTEGER NOT NULL,
    failed_parts INTEGER NOT NULL,
    throughput_yield DECIMAL(5,2) NOT NULL,
    week_id VARCHAR(10),           -- e.g., "2025-W20"
    week_start DATE,               -- Monday of the week
    week_end DATE,                 -- Sunday of the week
    total_starters INTEGER,        -- Parts that started this week
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (date_id, model, workstation_name)
);

-- Create weekly TPY metrics table
CREATE TABLE IF NOT EXISTS weekly_tpy_metrics (
    week_id VARCHAR(10) PRIMARY KEY,  -- "2025-W27"
    week_start DATE NOT NULL,
    week_end DATE NOT NULL,
    days_in_week INTEGER NOT NULL,
    
    -- Weekly First Pass Yield
    weekly_first_pass_yield_traditional_parts_started INTEGER,
    weekly_first_pass_yield_traditional_first_pass_success INTEGER,
    weekly_first_pass_yield_traditional_first_pass_yield DECIMAL(5,2),
    weekly_first_pass_yield_completed_only_active_parts INTEGER,
    weekly_first_pass_yield_completed_only_first_pass_success INTEGER,
    weekly_first_pass_yield_completed_only_first_pass_yield DECIMAL(5,2),
    weekly_first_pass_yield_breakdown_parts_completed INTEGER,
    weekly_first_pass_yield_breakdown_parts_failed INTEGER,
    weekly_first_pass_yield_breakdown_parts_stuck_in_limbo INTEGER,
    weekly_first_pass_yield_breakdown_total_parts INTEGER,
    
    -- Weekly Overall Yield
    weekly_overall_yield_total_parts INTEGER,
    weekly_overall_yield_completed_parts INTEGER,
    weekly_overall_yield_overall_yield DECIMAL(5,2),
    
    -- Weekly Throughput Yield (JSON stored as text)
    weekly_throughput_yield_station_metrics TEXT,  -- JSON string
    weekly_throughput_yield_average_yield DECIMAL(5,2),
    weekly_throughput_yield_model_specific TEXT,   -- JSON string
    
    -- Weekly TPY
    weekly_tpy_hardcoded_sxm4_stations TEXT,      -- JSON string
    weekly_tpy_hardcoded_sxm4_tpy DECIMAL(5,2),
    weekly_tpy_hardcoded_sxm5_stations TEXT,      -- JSON string
    weekly_tpy_hardcoded_sxm5_tpy DECIMAL(5,2),
    weekly_tpy_dynamic_sxm4_stations TEXT,        -- JSON string
    weekly_tpy_dynamic_sxm4_tpy DECIMAL(5,2),
    weekly_tpy_dynamic_sxm4_station_count INTEGER,
    weekly_tpy_dynamic_sxm5_stations TEXT,        -- JSON string
    weekly_tpy_dynamic_sxm5_tpy DECIMAL(5,2),
    weekly_tpy_dynamic_sxm5_station_count INTEGER,
    
    -- Summary
    total_stations INTEGER,
    best_station_name VARCHAR(50),
    best_station_yield DECIMAL(5,2),
    worst_station_name VARCHAR(50),
    worst_station_yield DECIMAL(5,2),
    
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create indexes for daily TPY metrics
CREATE INDEX IF NOT EXISTS idx_daily_tpy_date_model ON daily_tpy_metrics(date_id, model);
CREATE INDEX IF NOT EXISTS idx_daily_tpy_workstation ON daily_tpy_metrics(workstation_name);
CREATE INDEX IF NOT EXISTS idx_daily_tpy_week_id ON daily_tpy_metrics(week_id);

-- Create indexes for weekly TPY metrics
CREATE INDEX IF NOT EXISTS idx_weekly_tpy_week_start ON weekly_tpy_metrics(week_start);
CREATE INDEX IF NOT EXISTS idx_weekly_tpy_week_end ON weekly_tpy_metrics(week_end);

-- Add comments for documentation
COMMENT ON TABLE daily_tpy_metrics IS 'Daily throughput yield metrics by model and station';
COMMENT ON TABLE weekly_tpy_metrics IS 'Weekly aggregated TPY metrics with first pass yield and overall yield';

-- Add column comments for week-related fields
COMMENT ON COLUMN daily_tpy_metrics.week_id IS 'ISO week ID (e.g., 2025-W20)';
COMMENT ON COLUMN daily_tpy_metrics.week_start IS 'Start date of the week (Monday)';
COMMENT ON COLUMN daily_tpy_metrics.week_end IS 'End date of the week (Sunday)';
COMMENT ON COLUMN daily_tpy_metrics.total_starters IS 'Total parts that started this week'; 