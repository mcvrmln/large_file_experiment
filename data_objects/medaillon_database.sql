-- Proof of concept

use role accountadmin;

use warehouse compute_wh;

create database medaillon_o;

create schema bronze;

create or replace table bronze_table(
    id integer autoincrement(1000,1),
    source varchar(255),
    created_at timestamp_tz,
    updated_at timestamp_tz
)
enable_schema_evolution = TRUE;

create schema admin;

use schema admin;
create or replace file format my_csv_format
type = CSV
field_delimiter = ';'
parse_header = true
error_on_column_count_mismatch = false;

create or replace stage my_internal_stage;

use schema bronze;

create or replace table bronze_table(
    id integer autoincrement(1000,1),
    source varchar(255),
    created_at timestamp_tz,
    updated_at timestamp_tz
)
enable_schema_evolution = TRUE;

COPY INTO bronze.bronze_table
     FROM @admin.my_internal_stage
FILES = ( '20240306T20411709754110_file.csv' ) 
FILE_FORMAT = ( FORMAT_NAME = admin.my_csv_format)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

describe table bronze.bronze_table;

select * 
from bronze.bronze_table
limit 100;
