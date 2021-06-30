-- CONNECTION: name=postgres
DROP SCHEMA IF EXISTS information_mart CASCADE;
CREATE SCHEMA information_mart;
CREATE EXTENSION IF NOT EXISTS pgcrypto;