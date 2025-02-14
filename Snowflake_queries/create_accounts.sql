-- Use Account Admin role
use role accountadmin;

-- Create warehouse
create or replace warehouse dbt_wh with warehouse_size='small';

-- Create database
drop database if exists dbt_db;
create database dbt_db;

-- Create role
drop role if exists dbt_role;
create role dbt_role;

-- Grant privileges
grant usage on warehouse dbt_wh to role dbt_role;
grant all on database dbt_db to role dbt_role;

-- Set variable for current user
SET my_user = (SELECT CURRENT_USER());

-- Grant role using session variable
GRANT ROLE dbt_role TO USER IDENTIFIER($my_user);

-- Switch to dbt_role
use role dbt_role;

-- Create schema
create schema if not exists dbt_schema;
create schema if not exists json_schema;
create schema if not exists rdbms_schema;

-- Verify grants (optional)
show grants on warehouse dbt_wh;


