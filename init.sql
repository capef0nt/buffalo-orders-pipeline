-- Create Airflow DB 
CREATE DATABASE airflow;

-- Create Buffalo Orders DB
CREATE DATABASE buffalo_orders_db;

-- Give user access
GRANT ALL PRIVILEGES ON DATABASE airflow TO postgres;
GRANT ALL PRIVILEGES ON DATABASE buffalo_orders_db TO postgres;
