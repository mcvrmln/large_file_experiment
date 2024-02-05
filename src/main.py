""" The main body of this experiment """

import yaml

from utils import create_file
from utils import extract


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


def run_app():
    """The function where it all starts"""

    with open("./config/instructions.yml") as f:
        instructions = yaml.safe_load(f)

    # generate_large_file(instructions)
    process_large_file(instructions)


if __name__ == "__main__":
    run_app()
