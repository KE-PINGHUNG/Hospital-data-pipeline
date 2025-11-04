WITH appointments AS (
    SELECT
        appointment_id,
        patient_id,
        appointment_date::date AS visit_date, -- 提取日期作為就診日
        status AS appointment_status,
        department,
        doctor_id,
        -- 計算等候時間（如果 check_in_time 存在的話）
        EXTRACT(EPOCH FROM (check_in_time - appointment_date)) / 60 AS wait_time_minutes 
        -- 假設你的 stg_appointment 已經包含了 check_in_time 欄位
        -- 如果沒有，你需要先將 check_in_time 加入 stg_appointment 才能計算
    FROM {{ ref('stg_appointment') }}
),

diagnosis_summary AS (
    SELECT
        appointment_id,
        COUNT(diagnosis_id) AS total_diagnosis_codes,
        STRING_AGG(icd_code, '; ') AS all_icd_codes, -- 將所有 ICD 代碼合併為一個字串
        MIN(diagnosis_date) AS first_diagnosis_time
    FROM {{ ref('stg_diagnosis') }}
    GROUP BY 1
),

lab_summary AS (
    SELECT
        appointment_id,
        COUNT(lab_result_id) AS total_lab_tests,
        SUM(CASE WHEN is_abnormal THEN 1 ELSE 0 END) AS abnormal_lab_count,
        
        -- V V V V V V V V V V V V V V V V V V V V V V
        -- 修正：將布林值轉換為整數 (1/0) 後再取 MAX
        MAX(CAST(is_abnormal AS INT)) AS has_abnormal_result 
        -- ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^
        
    FROM {{ ref('stg_lab_results') }}
    GROUP BY 1
)

SELECT
    a.appointment_id,
    a.patient_id,
    a.visit_date,
    a.department,
    a.doctor_id,
    a.appointment_status,
    a.wait_time_minutes, -- 可用於計算急診等待時間
    d.total_diagnosis_codes,
    d.all_icd_codes,
    d.first_diagnosis_time,
    l.total_lab_tests,
    l.abnormal_lab_count,
    l.has_abnormal_result
FROM appointments a
LEFT JOIN diagnosis_summary d
    ON a.appointment_id = d.appointment_id
LEFT JOIN lab_summary l
    ON a.appointment_id = l.appointment_id
