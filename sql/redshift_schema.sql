-- Dimension: Customer Demographics
CREATE TABLE IF NOT EXISTS public.dim_customer (
    customerid VARCHAR PRIMARY KEY,
    city VARCHAR,
    zip_code INTEGER,
    gender VARCHAR,
    senior_citizen VARCHAR,
    partner VARCHAR,
    dependents VARCHAR
);

-- Dimension: Services subscribed
CREATE TABLE IF NOT EXISTS public.dim_services (
    customerid VARCHAR PRIMARY KEY,
    phone_service VARCHAR,
    multiple_lines VARCHAR,
    internet_service VARCHAR,
    online_security VARCHAR,
    online_backup VARCHAR,
    device_protection VARCHAR,
    tech_support VARCHAR,
    streaming_tv VARCHAR,
    streaming_movies VARCHAR
);

-- Dimension: Contract details
CREATE TABLE IF NOT EXISTS public.dim_contract (
    customerid VARCHAR PRIMARY KEY,
    contract VARCHAR,
    paperless_billing VARCHAR,
    payment_method VARCHAR
);

-- Fact: Churn metrics
CREATE TABLE IF NOT EXISTS public.fact_churn (
    customerid VARCHAR PRIMARY KEY,
    tenure_months INTEGER,
    monthly_charges DOUBLE PRECISION,
    total_charges DOUBLE PRECISION,
    churn_label VARCHAR,
    churn_value INTEGER,
    churn_score INTEGER,
    churn_reason VARCHAR
);

SELECT 'dim_customer' as table_name, COUNT(*) as rows FROM public.dim_customer
UNION ALL
SELECT 'dim_services', COUNT(*) FROM public.dim_services
UNION ALL
SELECT 'dim_contract', COUNT(*) FROM public.dim_contract
UNION ALL
SELECT 'fact_churn', COUNT(*) FROM public.fact_churn;