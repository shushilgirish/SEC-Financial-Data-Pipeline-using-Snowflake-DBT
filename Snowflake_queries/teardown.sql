-- Cleanup (Optional - remove if not needed)
use role accountadmin;
drop warehouse if exists dbt_wh;
drop database if exists dbt_db;
drop role if exists dbt_role;
