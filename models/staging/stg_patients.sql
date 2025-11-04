SELECT
    patient_id,
    full_name,
    gender,
    date_of_birth,
    -- PostgreSQL 中計算年齡
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, date_of_birth::date)) AS age
FROM {{ source('my_mysql', 'patients') }}
