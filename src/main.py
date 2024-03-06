""" The main body of this experiment """

import yaml
import datetime

from utils import create_file
from utils import extract
from utils import load


def generate_large_file(instructions: dict):
    """Generate a large file"""

    for file in instructions.keys():
        filename = create_file.generate_filename(file)
        create_file.generate_file(
            filename, instructions[file]["records"], instructions[file]["fields"]
        )


def process_large_file(instructions: dict):
    """Function to handle all the large file processing"""

    files = extract.get_filenames("./data", ".txt")
    file_defs = instructions.keys()
    for file in files:
        definition = extract.match_file_instructions(filename=file, file_defs=file_defs)
        file_definitions = extract.get_file_definitions(
            instructions[definition]["fields"]
        )
        df = extract.read_file(file, **file_definitions)
        extract.save_file(df, file)


def load_data_into_snowflake(config: dict, instructions: dict):
    """Function to load the data into snowflake"""

    files = extract.get_filenames("./data", ".parquet")

    # Upload files to stage and copy them into a table
    # cursor = load.create_cursor(config["user"], config["password"], config["account"])
    # load.load_files_in_stage(cursor, files, instructions)
    # load.copy_data_into_table(cursor, files, instructions)

    # Reads the files save the data directly into snowflake with pandas_tools
    connection = load.create_connection(
        config["user"], config["password"], config["account"], config["warehouse"]
    )
    load.load_dataframes_into_tables(connection, files, instructions)


def run_app():
    """The function where it all starts"""

    with open("./config/instructions.yml") as file:
        instructions = yaml.safe_load(file)

    with open("./config/config.yml", "r") as file:
        config = yaml.safe_load(file)
    print(f"Generate files: {datetime.datetime.now().strftime('%Y-%m%d %H:%M:%S.%f')}")
    generate_large_file(instructions)
    print(
        f"Process large files: {datetime.datetime.now().strftime('%Y-%m%d %H:%M:%S.%f')}"
    )
    process_large_file(instructions)
    print(
        f"Load data to Snowflake: {datetime.datetime.now().strftime('%Y-%m%d %H:%M:%S.%f')}"
    )
    load_data_into_snowflake(config["admin"], instructions)
    print(f"The end: {datetime.datetime.now().strftime('%Y-%m%d %H:%M:%S.%f')}")


if __name__ == "__main__":
    run_app()
