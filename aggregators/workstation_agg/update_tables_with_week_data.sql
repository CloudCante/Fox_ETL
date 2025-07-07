-- Add week metadata to daily_tpy_metrics table
ALTER TABLE daily_tpy_metrics 
ADD COLUMN IF NOT EXISTS week_id VARCHAR(10),
ADD COLUMN IF NOT EXISTS week_start DATE,
ADD COLUMN IF NOT EXISTS week_end DATE,
ADD COLUMN IF NOT EXISTS total_starters INTEGER;

-- Add week metadata to weekly_tpy_metrics table (already has week_start and week_end)
ALTER TABLE weekly_tpy_metrics 
ADD COLUMN IF NOT EXISTS week_id VARCHAR(10);

-- Create indexes for week data
CREATE INDEX IF NOT EXISTS idx_daily_tpy_week_id ON daily_tpy_metrics(week_id);
CREATE INDEX IF NOT EXISTS idx_weekly_tpy_week_id ON weekly_tpy_metrics(week_id);

-- Add comments
COMMENT ON COLUMN daily_tpy_metrics.week_id IS 'ISO week ID (e.g., 2025-W20)';
COMMENT ON COLUMN daily_tpy_metrics.week_start IS 'Start date of the week (Monday)';
COMMENT ON COLUMN daily_tpy_metrics.week_end IS 'End date of the week (Sunday)';
COMMENT ON COLUMN daily_tpy_metrics.total_starters IS 'Total parts that started this week';
COMMENT ON COLUMN weekly_tpy_metrics.week_id IS 'ISO week ID (e.g., 2025-W20)'; 