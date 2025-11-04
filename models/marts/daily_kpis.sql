SELECT
    visit_date,
    COUNT(appointment_id) AS total_daily_appointments,
    -- 計算急診科的平均等待時間
    ROUND(
        AVG(CASE WHEN department = '急診科' THEN wait_time_minutes ELSE NULL END), 2
    ) AS avg_emergency_wait_time_minutes,
    
    -- V V V V V V V V V V V V V V V V V V V V V V
    -- 修正：將布林判斷 (THEN) 改為數值判斷 (= 1)
    ROUND(
        SUM(CASE WHEN has_abnormal_result = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(appointment_id), 2
    ) AS daily_abnormal_lab_rate_percent
    -- ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^
    
FROM {{ ref('int_patient_visits') }}
GROUP BY 1
ORDER BY 1 DESC