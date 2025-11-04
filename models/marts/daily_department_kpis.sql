SELECT
    visit_date,
    department,
    COUNT(appointment_id) AS total_daily_appointments
FROM {{ ref('int_patient_visits') }}
GROUP BY 1, 2