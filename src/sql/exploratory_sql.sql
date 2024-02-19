/* SQL script to explore the data in Snowflake */

select count($1) from @GEWURZTRAMINER.BRONZE.LANDING_ZONE/20240205T18341707154467_test.parquet;


--create table gewurztraminer.bronze.example_table as
select $1:Field01::datetime,
    $1:Field02::varchar,
    $1:Field03::varchar,
    $1:Field04::integer,
    $1:Field05::datetime
from @GEWURZTRAMINER.BRONZE.LANDING_ZONE/20240204T22441707083088_file.parquet;

describe table gewurztraminer.bronze.example_table;
select * from gewurztraminer.bronze.example_table;


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
