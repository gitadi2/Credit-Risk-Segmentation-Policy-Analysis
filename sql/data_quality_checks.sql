CREATE TABLE credit.loan_data_staging_raw (
    id TEXT,
    year TEXT,
    loan_limit TEXT,
    gender TEXT,
    approv_in_adv TEXT,
    loan_type TEXT,
    loan_purpose TEXT,
    credit_worthiness TEXT,
    open_credit TEXT,
    business_or_commercial TEXT,
    loan_amount TEXT,
    rate_of_interest TEXT,
    interest_rate_spread TEXT,
    upfront_charges TEXT,
    term TEXT,
    neg_ammortization TEXT,
    interest_only TEXT,
    lump_sum_payment TEXT,
    property_value TEXT,
    construction_type TEXT,
    occupancy_type TEXT,
    secured_by TEXT,
    total_units TEXT,
    income TEXT,
    credit_type TEXT,
    credit_score TEXT,
    co_applicant_credit_type TEXT,
    age TEXT,
    submission_of_application TEXT,
    ltv TEXT,
    region TEXT,
    security_type TEXT,
    status TEXT,
    dtir1 TEXT,
    loan_to_income TEXT,
    loan_to_income_capped TEXT,
    credit_score_band TEXT,
    risk_score TEXT,
    risk_segment TEXT
);


\copy credit.loan_data_staging_raw FROM 'C:\Users\ADITYA SATAPATHY\OneDrive\Desktop\Credit Risk Segmentation Policy Analysis v2\data\processed\loan_data_with_risk_segments.csv' CSV HEADER;

CREATE TABLE credit.loan_data_staging (
    id BIGINT,
    income NUMERIC,
    loan_amount NUMERIC,
    credit_score INTEGER,
    ltv NUMERIC,
    dtir1 NUMERIC,
    loan_to_income NUMERIC,
    loan_to_income_capped NUMERIC,
    risk_score INTEGER,
    status INTEGER
);



INSERT INTO credit.loan_data_staging (
    id,
    income,
    loan_amount,
    credit_score,
    ltv,
    dtir1,
    loan_to_income,
    loan_to_income_capped,
    risk_score,
    status
)
SELECT
    id::BIGINT,
    income::NUMERIC,
    loan_amount::NUMERIC,
    credit_score::INTEGER,
    ltv::NUMERIC,
    dtir1::NUMERIC,
    loan_to_income::NUMERIC,
    loan_to_income_capped::NUMERIC,
    risk_score::INTEGER,
    status::INTEGER
FROM credit.loan_data_staging_raw
WHERE
    income IS NOT NULL
    AND loan_amount IS NOT NULL
    AND credit_score IS NOT NULL
    AND ltv IS NOT NULL
    AND dtir1 IS NOT NULL;


SELECT COUNT(*) FROM credit.loan_data_staging;


SELECT * FROM credit.loan_data_staging LIMIT 5;


CREATE TABLE credit.loan_data_with_risk_segments AS
SELECT
    id,
    income,
    loan_amount,
    credit_score,
    ltv,
    dtir1,
    loan_to_income,
    loan_to_income_capped,
    risk_score,
    status,
    CASE
        WHEN credit_score >= 740 
             AND ltv <= 80 
             AND dtir1 <= 40 
        THEN 'Low Risk'

        WHEN credit_score >= 670 
             AND ltv <= 90 
             AND dtir1 <= 50 
        THEN 'Medium Risk'

        ELSE 'High Risk'
    END AS risk_segment
FROM credit.loan_data_staging;


SELECT COUNT(*) FROM credit.loan_data_with_risk_segments;


SELECT risk_segment, COUNT(*) 
FROM credit.loan_data_with_risk_segments
GROUP BY risk_segment;


SELECT
    COUNT(*) AS total_rows,
    COUNT(income) AS income_present,
    COUNT(loan_amount) AS loan_amount_present,
    COUNT(credit_score) AS credit_score_present,
    COUNT(ltv) AS ltv_present,
    COUNT(dtir1) AS dtir1_present,
    COUNT(risk_segment) AS risk_segment_present
FROM credit.loan_data_with_risk_segments;


SELECT
    SUM(CASE WHEN credit_score < 300 OR credit_score > 900 THEN 1 ELSE 0 END) 
        AS invalid_credit_score,
    SUM(CASE WHEN ltv <= 0 OR ltv > 200 THEN 1 ELSE 0 END) 
        AS invalid_ltv,
    SUM(CASE WHEN dtir1 < 0 OR dtir1 > 100 THEN 1 ELSE 0 END) 
        AS invalid_dtir
FROM credit.loan_data_with_risk_segments;


SELECT
    risk_segment,
    MIN(credit_score) AS min_credit_score,
    MAX(credit_score) AS max_credit_score,
    MIN(ltv) AS min_ltv,
    MAX(ltv) AS max_ltv,
    MIN(dtir1) AS min_dtir,
    MAX(dtir1) AS max_dtir
FROM credit.loan_data_with_risk_segments
GROUP BY risk_segment;


SELECT
    COUNT(*) AS total_applications,
    SUM(CASE WHEN risk_segment IN ('Low Risk', 'Medium Risk') THEN 1 ELSE 0 END) AS approved,
    SUM(CASE WHEN risk_segment = 'High Risk' THEN 1 ELSE 0 END) AS rejected
FROM credit.loan_data_with_risk_segments;


SELECT
    risk_segment,
    COUNT(*) AS approved_count
FROM credit.loan_data_with_risk_segments
WHERE risk_segment IN ('Low Risk', 'Medium Risk')
GROUP BY risk_segment;


SELECT
    COUNT(*) AS approved_low_risk_only
FROM credit.loan_data_with_risk_segments
WHERE risk_segment = 'Low Risk';


SELECT COUNT(*) AS total_approved
FROM credit.loan_data_with_risk_segments;


SELECT
    'Strict Policy (Low Risk Only)' AS policy,
    COUNT(*) AS approved
FROM credit.loan_data_with_risk_segments
WHERE risk_segment = 'Low Risk'

UNION ALL

SELECT
    'Baseline Policy (Low + Medium Risk)',
    COUNT(*)
FROM credit.loan_data_with_risk_segments
WHERE risk_segment IN ('Low Risk', 'Medium Risk')

UNION ALL

SELECT
    'Aggressive Policy (All Approved)',
    COUNT(*)
FROM credit.loan_data_with_risk_segments;



SELECT
    risk_segment,
    COUNT(*) AS approved_loans,
    ROUND(AVG(loan_amount), 2) AS avg_loan_amount,
    ROUND(SUM(loan_amount), 2) AS total_disbursed_amount
FROM credit.loan_data_with_risk_segments
WHERE risk_segment IN ('Low Risk', 'Medium Risk')
GROUP BY risk_segment;


SELECT
    'Strict Policy (Low Risk Only)' AS policy,
    COUNT(*) AS loans,
    ROUND(SUM(loan_amount), 2) AS total_disbursed
FROM credit.loan_data_with_risk_segments
WHERE risk_segment = 'Low Risk'

UNION ALL

SELECT
    'Baseline Policy (Low + Medium)',
    COUNT(*),
    ROUND(SUM(loan_amount), 2)
FROM credit.loan_data_with_risk_segments
WHERE risk_segment IN ('Low Risk', 'Medium Risk')

UNION ALL

SELECT
    'Aggressive Policy (All Approved)',
    COUNT(*),
    ROUND(SUM(loan_amount), 2)
FROM credit.loan_data_with_risk_segments;

