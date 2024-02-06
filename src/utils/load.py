"""" Functions for loading the data into Snowflake """

import snowflake.connector


def create_cursor(user: str, password: str, account: str):
    """Creates a Snowflake connection"""

    connection = snowflake.connector.connect(
        user=user, password=password, account=account
    )

    return connection.cursor()


def load_files_in_stage(files: list, database: str, schema: str, stage: str):
    """Loads a list of files (parquet files) into the stage"""

    pass
