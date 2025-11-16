CREATE TABLE IF NOT EXISTS anomalies_raw (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(50),
    timestamp_recorded TIMESTAMPTZ,
    temperature NUMERIC(5,2),
    reason VARCHAR(255)
);