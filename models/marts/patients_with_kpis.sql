WITH appointments_agg AS (
    -- 聚合 appointments 資料，計算每個病人的 KPIs
    SELECT
        patient_id,
        COUNT(appointment_id) AS total_appointments,
        MIN(appointment_date) AS first_appointment_date,
        MAX(appointment_date) AS latest_appointment_date,
        SUM(CASE WHEN appointment_status_cn = '已完成' THEN 1 ELSE 0 END) AS completed_appointments
    FROM {{ ref('stg_appointment') }}
    GROUP BY 1
),

final_join AS (
    -- 將病患的基本資訊與聚合的 KPI 數據合併
    SELECT
        p.patient_id,
        p.full_name,
        p.gender,
        p.age,
        p.date_of_birth,
        a.total_appointments,
        a.first_appointment_date,
        a.latest_appointment_date,
        a.completed_appointments
    FROM {{ ref('stg_patients') }} p
    LEFT JOIN appointments_agg a
        ON p.patient_id = a.patient_id
)

SELECT * FROM final_join
-- 確保只返回至少有一次預約的病人
WHERE total_appointments IS NOT NULL
