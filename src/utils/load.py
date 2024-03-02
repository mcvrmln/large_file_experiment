"""" Functions for loading the data into Snowflake """

import snowflake.connector


def create_cursor(user: str, password: str, account: str):
    """Creates a Snowflake connection"""

    connection = snowflake.connector.connect(
        user=user, password=password, account=account
    )

    return connection.cursor()


def load_files_in_stage(cursor, files: list, instructions: dict):
    """Loads a list of files (parquet files) into the internal named stage"""

    for file in files:
        database, schema, stage, table = determine_target(file, instructions)
        print(f"{database=} --> {schema =} --> {stage =}")
        sql = f"put file://{file} @{database}.{schema}.{stage} overwrite=FALSE;"
        result = cursor.execute(sql)
        print("File is uploaded")
        print(result.is_file_transfer, result.sqlstate, result.sfqid)


def copy_data_into_table(cursor, files: list, instructions: dict):
    """Copy data into table"""

    for file in files:
        database, schema, stage, table = determine_target(file, instructions)
        sql = f"use database {database};"
        cursor.execute(sql)
        sql = f"use schema {schema};"
        cursor.execute(sql)
        sql = f"copy into {database}.{schema}.{table} from @{database}.{schema}.{stage} MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE';"
        result = cursor.execute(sql)
        print(result.sqlstate)


def determine_target(file_name: str, instructions: dict) -> tuple:
    """Based on the instructions, determine the right stage to load the data into Snowflake"""

    for key, value in instructions.items():
        if file_name.count(key):
            database = instructions[key]["database"]
            schema = instructions[key]["schema"]
            stage = instructions[key]["stage"]
            table = instructions[key]["table"]

    return database, schema, stage, table
