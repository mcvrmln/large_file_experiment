/* SQL script to explore the data in Snowflake */

select count($1) from @GEWURZTRAMINER.BRONZE.LANDING_ZONE/20240205T18341707154467_test.parquet;

truncate gewurztraminer.bronze.example_table;
create table gewurztraminer.bronze.example_table as
select $1:Field01::varchar::date as Field01,
    $1:Field02::varchar as Field02,
    $1:Field03::varchar as Field03,
    $1:Field04::integer as Field04,
    $1:Field05::varchar::date as Field05
from @GEWURZTRAMINER.BRONZE.LANDING_ZONE/20240220T21201708460426_file.parquet;

copy into gewurztraminer.bronze.example_table
from (
    select $1:Field01::varchar::date,
        $1:Field02::varchar,
        $1:Field03::varchar,
        $1:Field04::integer,
        $1:Field05::varchar::date
    from @GEWURZTRAMINER.BRONZE.LANDING_ZONE
);


describe table gewurztraminer.bronze.example_table;
select 
    field05 - field01 as difference_in_dates,
    count(*) as number_observations
from gewurztraminer.bronze.example_table
group by all
order by number_observations desc;


drop table gewurztraminer.bronze.example_table;

SELECT *
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION=>'@GEWURZTRAMINER.BRONZE.LANDING_ZONE'
      , FILE_FORMAT=>'parquetfile'
      )
    );

CREATE TABLE example_table
  USING TEMPLATE (
    SELECT *
      FROM TABLE(
        INFER_SCHEMA(
          LOCATION=>'@GEWURZTRAMINER.BRONZE.LANDING_ZONE/*file.parquet',
          FILE_FORMAT=>'parquetfile'
        )
      ));




SELECT GENERATE_COLUMN_DESCRIPTION(ARRAY_AGG(OBJECT_CONSTRUCT(*)), 'table') AS COLUMNS
  FROM TABLE (
    INFER_SCHEMA(
      LOCATION=>'@GEWURZTRAMINER.BRONZE.LANDING_ZONE/20240220T21201708460426_file.parquet',
      FILE_FORMAT=>'parquetfile'
    )
  );

  create or replace table gewurztraminer.bronze.example_table2
  (
    "Field01" NUMBER(38, 0),
"Field02" TEXT,
"Field03" TEXT,
"Field04" NUMBER(38, 0),
"Field05" NUMBER(38, 0)
  );

use database gewurztraminer;
use schema bronze;
  copy into gewurztraminer.bronze.example_table4
  from @gewurztraminer.bronze.landing_zone
  MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE';

  select field01::varchar::date as date_field01, count(*) as number_dates 
  from example_table4
  group by all
  order by number_dates desc;
  
  list @gewurztraminer.bronze.landing_zone;
  remove @gewurztraminer.bronze.landing_zone;
  select * from gewurztraminer.bronze.example_table4;


create or replace table gewurztraminer.bronze.example_table3
  (
    Field01 variant,
    Field02 variant,
    Field03 variant,
    Field04 variant,
    Field05 variant
  );


create or replace table gewurztraminer.bronze.example_table4
  (
    Field01 variant,
    Field02 variant,
    Field03 variant,
    FieldAA variant, 
    Field04 variant,
    Field05 variant
  );


select count(*)
from gewurztraminer.bronze.levure
limit 1000;
