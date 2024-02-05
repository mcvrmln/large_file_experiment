""" The main body of this experiment """

import yaml

from utils import create_file


def generate_large_file():
    """Generate a large file"""

    with open("./instructions.yml") as f:
        instructions = yaml.safe_load(f)

    # print(len(instructions['file']['fields']))

    for file in instructions.keys():
        filename = create_file.generate_filename(file)
        create_file.generate_file(
            filename, instructions[file]["records"], instructions[file]["fields"]
        )


def process_large_file():
    """Function to handle all the large file processing"""

    pass


def run_app():
    """The function where it all starts"""

    generate_large_file()
    process_large_file()


if __name__ == "__main__":
    run_app()
