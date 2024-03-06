"""" Functions for loading the data into Snowflake """

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import datetime


def create_connection(user: str, password: str, account: str, warehouse: str):
    """Creates a Snowflake connection"""

    return snowflake.connector.connect(
        user=user, password=password, account=account, warehouse=warehouse
    )


def save_dataframe(connection, dataframe, table, database, schema):
    """
    Save a dataframe directly into Snowflake

    More information: https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api#write_pandas
    """
    # connection.cursor().execute(f"USE DATABASE {database};")
    # connection.cursor().execute(f"USE SCHEMA {schema};")

    return write_pandas(
        conn=connection,
        df=dataframe,
        table_name=table,
        database=database,
        schema=schema,
        quote_identifiers=False,
        use_logical_type=True,
    )


def load_dataframes_into_tables(connection, files, instructions):
    """Iterate over files"""

    for file in files:
        database, schema, stage, table = determine_target(file, instructions)
        df = pd.read_parquet(file)
        print(
            f"Write panda to Snowflake: {datetime.datetime.now().strftime('%Y-%m%d %H:%M:%S.%f')}"
        )
        print(file)
        print(df.info())
        result = save_dataframe(
            connection=connection,
            dataframe=df,
            database=database,
            schema=schema,
            table=table,
        )
        print(result)


def create_cursor(user: str, password: str, account: str):
    """Creates a Snowflake cursor"""

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
