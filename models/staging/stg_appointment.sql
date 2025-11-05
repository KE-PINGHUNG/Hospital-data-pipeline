SELECT
    -- 將 UUID 類型的主鍵轉換為字串 (PostgreSQL)
    CAST(appointment_id AS VARCHAR) AS appointment_id,
    CAST(patient_id AS VARCHAR) AS patient_id,

    -- 確保日期類型正確
    CAST(appointment_date AS TIMESTAMP) AS appointment_date,
    department,
    doctor_id,
    check_in_time,
    status,
    -- 根據數據 (Completed, NoShow, Cancelled) 進行中文轉換
    CASE status
        WHEN 'Completed' THEN '已完成'
        WHEN 'NoShow'    THEN '未出席'
        WHEN 'Cancelled' THEN '已取消'
        ELSE '未知'
    END AS appointment_status_cn
FROM {{ source('my_mysql', 'appointment') }}
