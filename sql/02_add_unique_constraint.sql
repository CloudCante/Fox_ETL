ALTER TABLE master_records
ADD CONSTRAINT unique_record UNIQUE (
    sn, 
    workstation_name, 
    history_station_start_time, 
    history_station_end_time, 
    data_source
); 