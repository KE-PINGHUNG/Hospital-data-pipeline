-- models/staging/stg_diagnosis.sql
SELECT
    CAST(diagnosis_id AS VARCHAR) AS diagnosis_id,
    CAST(appointment_id AS VARCHAR) AS appointment_id,
    icd_code,
    diagnosis_desc,
    CAST(diagnosis_date AS TIMESTAMP) AS diagnosis_date
FROM {{ source('my_mysql', 'diagnosis') }}
