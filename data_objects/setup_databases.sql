create database if not exists gewurztraminer;
use database gewurztraminer;
create schema if not exists bronze;

create warehouse cremant
with warehouse_size =  XSMALL;

create database if not exists pinot_noir;

use database pinot_noir;

create schema if not exists bronze;

create warehouse if not exists tardive
with warehouse_size =  XSMALL;

-- 

use database gewurztraminer;
use schema bronze;

-- https://docs.snowflake.com/en/sql-reference/sql/create-file-format
create or replace file format parquetfile
type = parquet
trim_space = True;

-- https://docs.snowflake.com/en/sql-reference/sql/create-stage
create or replace stage landing_zone
file_format = parquetfile;

--

use database pinot_noir;
use schema bronze;

-- https://docs.snowflake.com/en/sql-reference/sql/create-file-format
create or replace file format parquetfile
type = parquet
trim_space = True;

-- https://docs.snowflake.com/en/sql-reference/sql/create-stage
create or replace stage landing_zone
file_format = parquetfile;
