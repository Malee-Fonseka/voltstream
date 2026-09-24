-- Airflow's metadata database (T120, D6).
--
-- A separate database on the same instance — not a separate schema, and emphatically not
-- the application database. Airflow runs its own migrations and owns its tables entirely;
-- sharing a database would mean an Airflow upgrade taking locks on, or migrating around,
-- the billing tables. One Postgres instance is enough at this scale; one database is not.
--
-- Numbered 00 so it runs before the application schema. Like every other file in this
-- directory it is executed by the `postgres-init` container on **every** `compose up`
-- (not from docker-entrypoint-initdb.d, which would fire only on a fresh volume), so it
-- has to be idempotent. `CREATE DATABASE` cannot be wrapped in `IF NOT EXISTS` the way
-- the table definitions are, and cannot run inside a transaction block, so the existence
-- check is done in a query and the statement is executed through `\gexec` only when the
-- database is absent.

SELECT 'CREATE DATABASE airflow OWNER ' || quote_ident(current_user)
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
