-- Create master_records table for unified data storage
-- This table will hold both testboard and workstation data

CREATE TABLE IF NOT EXISTS master_records (
    id SERIAL PRIMARY KEY,
    
    -- Core identification fields
    sn VARCHAR(255) NOT NULL,
    pn VARCHAR(255),
    model VARCHAR(255),
    
    -- Testboard-specific fields
    work_station_process VARCHAR(255),
    baseboard_sn VARCHAR(255),
    baseboard_pn VARCHAR(255),
    number_of_times_baseboard_is_used INTEGER,
    
    -- Workstation-specific fields
    customer_pn VARCHAR(255),
    outbound_version VARCHAR(255),
    hours VARCHAR(255),
    service_flow VARCHAR(255),
    passing_station_method VARCHAR(255),
    first_station_start_time TIMESTAMP,
    day INTEGER,
    
    -- Common fields
    workstation_name VARCHAR(255) NOT NULL,
    history_station_start_time TIMESTAMP NOT NULL,
    history_station_end_time TIMESTAMP NOT NULL,
    history_station_passing_status VARCHAR(255),
    operator VARCHAR(255),
    
    -- Failure-related fields (mainly from testboard)
    failure_reasons TEXT,
    failure_note TEXT,
    failure_code VARCHAR(255),
    diag_version VARCHAR(255),
    fixture_no VARCHAR(255),
    
    -- Data source tracking
    data_source VARCHAR(50) NOT NULL,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create unique constraint to prevent duplicates
-- This ensures we don't get duplicate records for the same test at the same station
CREATE UNIQUE INDEX IF NOT EXISTS idx_master_records_unique 
ON master_records (sn, workstation_name, history_station_start_time, history_station_end_time, data_source);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_master_records_sn ON master_records(sn);
CREATE INDEX IF NOT EXISTS idx_master_records_model ON master_records(model);
CREATE INDEX IF NOT EXISTS idx_master_records_workstation ON master_records(workstation_name);
CREATE INDEX IF NOT EXISTS idx_master_records_data_source ON master_records(data_source);
CREATE INDEX IF NOT EXISTS idx_master_records_start_time ON master_records(history_station_start_time);
CREATE INDEX IF NOT EXISTS idx_master_records_passing_status ON master_records(history_station_passing_status);

-- Add comments for documentation
COMMENT ON TABLE master_records IS 'Unified table for all test records from testboard and workstation reports';
COMMENT ON COLUMN master_records.data_source IS 'Source of the data: testboard or workstation';
COMMENT ON COLUMN master_records.sn IS 'Serial number of the device being tested';
COMMENT ON COLUMN master_records.workstation_name IS 'Name of the test station';
COMMENT ON COLUMN master_records.history_station_start_time IS 'When the test started at this station';
COMMENT ON COLUMN master_records.history_station_end_time IS 'When the test ended at this station'; 