""" Some general functions """

import datetime


def generate_filename(name: str) -> str:
    """ Generate filename. """

    date = datetime.datetime.now()
    date = date.strftime('%Y%m%dT%H%M%s')
    name = '_' + str(name)
    extension = '.txt'
    filename = str(date) + name + extension
    
    return filename

