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
trim_space = True
USE_LOGICAL_TYPE = TRUE;

-- https://docs.snowflake.com/en/sql-reference/sql/create-stage
create or replace stage landing_zone
file_format = parquetfile;

create or replace table gewurztraminer.bronze.levure (
    ID integer autoincrement(1000,1),
    Field01 timestamp_ltz,
    Field02 varchar(20),
    Field03 varchar,
    Field04 integer,
    Field05 timestamp_ltz,
    source varchar(255),
    create_dts timestamp_ltz
);



create pipe cuve 
    as 
    copy into levure 
    from @gewurztraminer.bronze.landing_zone
    MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE';

--

use database pinot_noir;
use schema bronze;

-- https://docs.snowflake.com/en/sql-reference/sql/create-file-format
create or replace file format parquetfile
type = parquet
trim_space = True
USE_LOGICAL_TYPE = TRUE;

-- https://docs.snowflake.com/en/sql-reference/sql/create-stage
create or replace stage landing_zone
file_format = parquetfile;

create or replace table pinot_noir.bronze.bouchon (
    ID integer autoincrement(1000,1),
    Field_x1 integer,
    Field_x2 varchar(100),
    source varchar(255),
    create_dts timestamp_ltz
);
