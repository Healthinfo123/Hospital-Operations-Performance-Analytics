-- =======================================================
--   HOSPITAL ANALYTICS PROJECT - SQL ANALYSIS QUERIES
--   Author: Siri Naredla
--   Description: Analysis SQL for LOS, diagnoses,
--                documentation quality, and medications.
-- =======================================================


-- -------------------------------------------------------
-- 0. BASIC DATA EXPLORATION
-- -------------------------------------------------------

-- Count records in each table
SELECT COUNT(*) AS total_patients FROM patients;
SELECT COUNT(*) AS total_diagnoses FROM diagnoses;
SELECT COUNT(*) AS total_docs FROM documentation_log;
SELECT COUNT(*) AS total_meds FROM medications;

-- Preview samples
SELECT * FROM patients LIMIT 10;
SELECT * FROM diagnoses LIMIT 10;
SELECT * FROM documentation_log LIMIT 10;
SELECT * FROM medications LIMIT 10;



-- -------------------------------------------------------
-- 1. LENGTH OF STAY (LOS) ANALYSIS
-- -------------------------------------------------------

-- LOS basic distribution
SELECT
    MIN(LOS) AS min_los,
    MAX(LOS) AS max_los,
    AVG(LOS) AS avg_los,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY LOS) AS median_los
FROM patients;


-- LOS by diagnosis category
SELECT
    d.diagnosis_category,
    AVG(p.LOS) AS avg_los,
    COUNT(*) AS total_cases
FROM patients p
LEFT JOIN diagnoses d
    ON p.primary_diagnosis_code = d.diagnosis_code
GROUP BY d.diagnosis_category
ORDER BY avg_los DESC;



-- -------------------------------------------------------
-- 2. DOCUMENTATION QUALITY IMPACT
-- -------------------------------------------------------

-- Missing documentation counts
SELECT
    SUM(missing_progress_note) AS missing_progress_notes,
    SUM(missing_discharge_summary) AS missing_discharge_summaries
FROM documentation_log;


-- Impact of discharge summary delay on LOS
SELECT
    d.discharge_summary_delay_hours,
    AVG(p.LOS) AS avg_los
FROM patients p
LEFT JOIN documentation_log d
    ON p.encounter_id = d.encounter_id
GROUP BY d.discharge_summary_delay_hours
ORDER BY d.discharge_summary_delay_hours;



-- Bucketed delay analysis
WITH delay_bucket AS (
    SELECT
        p.LOS,
        CASE
            WHEN d.discharge_summary_delay_hours <= 0 THEN 'No delay'
            WHEN d.discharge_summary_delay_hours <= 24 THEN '0-24h'
            WHEN d.discharge_summary_delay_hours <= 72 THEN '24-72h'
            WHEN d.discharge_summary_delay_hours <= 168 THEN '3-7 days'
            ELSE '>7 days'
        END AS bucket
    FROM patients p
    LEFT JOIN documentation_log d
        ON p.encounter_id = d.encounter_id
)
SELECT
    bucket,
    COUNT(*) AS total_cases,
    AVG(LOS) AS avg_los
FROM delay_bucket
GROUP BY bucket
ORDER BY avg_los DESC;



-- -------------------------------------------------------
-- 3. MEDICATION IMPACT ANALYSIS
-- -------------------------------------------------------

-- Count medications per encounter
SELECT
    encounter_id,
    COUNT(*) AS medication_count
FROM medications
GROUP BY encounter_id
ORDER BY medication_count DESC;


-- LOS by therapeutic class
SELECT
    m.therapeutic_class,
    AVG(p.LOS) AS avg_los,
    COUNT(*) AS total_cases
FROM patients p
LEFT JOIN medications m
    ON p.encounter_id = m.encounter_id
GROUP BY m.therapeutic_class
ORDER BY avg_los DESC;



-- -------------------------------------------------------
-- 4. ENCOUNTER-LEVEL SUMMARY (FULL MERGE)
-- -------------------------------------------------------

SELECT
    p.encounter_id,
    p.patient_id,
    p.LOS,
    d.diagnosis_category,
    doc.missing_progress_note,
    doc.missing_discharge_summary,
    doc.discharge_summary_delay_hours,
    COUNT(m.medication_name) AS med_count
FROM patients p
LEFT JOIN diagnoses d
    ON p.primary_diagnosis_code = d.diagnosis_code
LEFT JOIN documentation_log doc
    ON p.encounter_id = doc.encounter_id
LEFT JOIN medications m
    ON p.encounter_id = m.encounter_id
GROUP BY
    p.encounter_id, p.patient_id, p.LOS, d.diagnosis_category,
    doc.missing_progress_note, doc.missing_discharge_summary,
    doc.discharge_summary_delay_hours
ORDER BY p.LOS DESC;


-- -------------------------------------------------------
-- END OF FILE
-- -------------------------------------------------------
