CREATE ROLE postgres LOGIN SUPERUSER PASSWORD 'postgres';
CREATE DATABASE company01;
GRANT ALL PRIVILEGES ON DATABASE company01 to airflow;
