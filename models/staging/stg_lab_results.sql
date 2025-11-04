SELECT
    CAST(result_id AS VARCHAR) AS lab_result_id, -- 統一命名為 lab_result_id
    CAST(appointment_id AS VARCHAR) AS appointment_id,
    lab_test_name,
    test_result_value,
    unit,
    result_text,
    CAST(test_date AS TIMESTAMP) AS test_date,
    -- 將檢驗結果的狀態統一為簡潔的布林值或標籤
    CASE
        WHEN result_text LIKE '異常%' THEN TRUE
        ELSE FALSE
    END AS is_abnormal
FROM {{ source('my_mysql', 'lab_result') }}
