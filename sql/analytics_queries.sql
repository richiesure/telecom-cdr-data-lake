-- Analytics Queries

-- Revenue by call type
SELECT 
    call_type,
    COUNT(*) as total_calls,
    ROUND(SUM(billing_amount), 2) as revenue
FROM telecom_data_lake.cdr_records
GROUP BY call_type;

-- Daily trend
SELECT 
    call_date,
    COUNT(*) as calls,
    ROUND(SUM(billing_amount), 2) as revenue
FROM telecom_data_lake.cdr_records
GROUP BY call_date
ORDER BY call_date DESC;

-- Failed calls
SELECT 
    cell_tower_id,
    COUNT(CASE WHEN call_status = 'failed' THEN 1 END) as failed_calls
FROM telecom_data_lake.cdr_records
GROUP BY cell_tower_id
ORDER BY failed_calls DESC
LIMIT 10;
