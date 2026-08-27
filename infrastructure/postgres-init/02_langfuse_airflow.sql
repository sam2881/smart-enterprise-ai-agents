-- Create additional databases for platform services
-- Runs as postgres superuser during container initialization

SELECT 'CREATE DATABASE langfuse OWNER admin'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'langfuse')\gexec

SELECT 'CREATE DATABASE airflow OWNER admin'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

GRANT ALL PRIVILEGES ON DATABASE langfuse TO admin;
GRANT ALL PRIVILEGES ON DATABASE airflow TO admin;
