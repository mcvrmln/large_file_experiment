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
        database, schema, stage = determine_target(file, instructions)
        print(f"{database=} --> {schema =} --> {stage =}")
        sql = f"put file://{file} @{database}.{schema}.{stage} overwrite=TRUE;"
        result = cursor.execute(sql)
        print("File is uploaded")
        print(result.is_file_transfer, result.sqlstate, result.sfqid)


def determine_target(file_name: str, instructions: dict) -> tuple:
    """Based on the instructions, determine the right stage to load the data into Snowflake"""

    for key, value in instructions.items():
        if file_name.count(key):
            database = instructions[key]["database"]
            schema = instructions[key]["schema"]
            stage = instructions[key]["stage"]

    return database, schema, stage
