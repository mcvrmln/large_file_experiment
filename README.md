# An experiment with large files

Steps to execute

- [x] Create a large fixed record lenght file (> 2 Gb)
- [x] Process this file (split in columns)
- [x] create database, schema, file format and stages (Snowflake)
- [] Put it in Snowflake
- [] Create Snowpipe
- [] Use Snowflake API to execute pipe
- [] Users in Snowflake
- [] Split file in chunks (100 - 250 Mb))
- [] Validate each record


## Create a large file
The file is generated based on the file specifications in the instructions. Only three types are allowed: strings, integers and dates. The dates are formatted as YYYYMMDD (8 characters).

## Split in columns
The heavy lifting is done with pandas. Datatypes are defined in the instructions.yml file.
