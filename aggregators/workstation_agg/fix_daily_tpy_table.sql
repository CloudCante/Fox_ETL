CREATE TABLE IF NOT EXISTS daily_tpy_metrics (
    date_id DATE NOT NULL,
    model VARCHAR(20) NOT NULL,
    workstation_name VARCHAR(50) NOT NULL,
    total_parts INTEGER NOT NULL,
    passed_parts INTEGER NOT NULL,
    failed_parts INTEGER NOT NULL,
    throughput_yield DECIMAL(5,2) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (date_id, model, workstation_name)
);

CREATE INDEX IF NOT EXISTS idx_daily_tpy_date_model ON daily_tpy_metrics(date_id, model);
CREATE INDEX IF NOT EXISTS idx_daily_tpy_workstation ON daily_tpy_metrics(workstation_name);

COMMENT ON TABLE daily_tpy_metrics IS 'Daily throughput yield metrics by model and station'; 