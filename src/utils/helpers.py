""" Some general functions """

import datetime
import time
import os
import string
import random


def generate_filename(name: str) -> str:
    """ Generate filename. """

    date = datetime.datetime.now()
    date = date.strftime('%Y%m%dT%H%M%s')
    name = '_' + str(name)
    extension = '.txt'
    filename = str(date) + name + extension
    
    return filename

def generate_file(filename, number_records, fields):
    """ Generate file based on the provided fields. """
    

    with open(os.path.join('./data', filename), 'w') as file:
        for i in range(number_records):
            file.write(generate_record(fields))

    
def generate_record(fields):
    """ Generate a record based on the fields provided """

    text = ''

    for field in fields:
        if field['field']['type'] == 'string':
            text += generate_string(field['field']['length'])
        elif field['field']['type'] == 'integer':
            text += generate_integer(field['field']['length'])
        elif field['field']['type'] == 'date':
            text += generate_date()
        else:
            text += ' ' # Just add a space
    
    return text + '\n'

def generate_string(length):
    """ Generate a string """
   
    return ''.join(random.choices(string.ascii_letters + string.digits + ' ', k=length))


def generate_integer(length):
    """ Generate an integer """

    return ''.join(random.choices(string.digits, k=length))



def generate_date():
    """ Generate a date between 1970-01-01 and today """

    date = datetime.datetime.now()
    unix_time = int(time.mktime(date.timetuple()))
    random_unix_time = random.randint(0, unix_time)
    random_date = datetime.datetime.fromtimestamp(random_unix_time)

    return random_date.strftime('%Y%m%d')
