""" The main body of this experiment """

import yaml

from utils import helpers


def generate_large_file():
    """ 
    Generate a large file 
    
    This function will be too long. It will contain all the steps to generate a large file.
    """
    
    
    with open('./instructions.yml') as f:
        instructions = yaml.safe_load(f)
    
    filename = helpers.generate_filename('TODO')
    print(list(instructions.keys()))


def process_large_file():
    """ Function to handle all the large file processing """

    pass
    


def run_app():
    """ The function where it all starts """
    
    generate_large_file()
    process_large_file()



if __name__ == "__main__":
    run_app()
