""" Extract the data out of the file """

import os
import pandas as pd
from datetime import datetime


def get_filenames(folder: str, file_type: str) -> list:
    """Reads the folder and returns all the files with matching extensions in this folder"""

    file_list = []

    for root, dirs, files in os.walk(folder):
        # select file name
        for file in files:
            if file.endswith(file_type):
                file_list.append(os.path.join(root, file))

    return file_list


def match_file_instructions(filename: str, file_defs: list) -> str:
    """The filename must contain one of the keys of the instructions"""

    for definition in file_defs:
        if definition in filename:
            return definition

    return None


def get_file_definitions(fields: list):
    """Transforms the information in the instructions to required parameters for pandas"""

    column_names = []
    widths = []
    column_types = {}
    date_columns = []

    for field in fields:
        column_names.append(field["field"]["name"])
        widths.append(field["field"]["length"])
        if field["field"]["type"] == "integer":
            column_types[field["field"]["name"]] = "Int64"
        else:
            column_types[field["field"]["name"]] = str
        if field["field"]["type"] == "date":
            date_columns.append(field["field"]["name"])

    file_definitions = {
        "column_names": column_names,
        "widths": widths,
        "column_types": column_types,
        "date_columns": date_columns,
    }
    return file_definitions


def read_file(
    filename: str,
    column_names: list[str],
    widths: list[int],
    column_types: dict,
    date_columns: list,
) -> pd.DataFrame:
    """Read the fixed width file with pandas"""

    df = pd.read_fwf(filename, widths=widths, names=column_names, dtype=column_types)
    for column in date_columns:
        df[column] = pd.to_datetime(df[column], format="%Y%m%d")
        df[column] = df[column].dt.tz_localize("UTC")

    df["source"] = filename
    df["create_dts"] = datetime.now()
    return df


def save_file(df: pd.DataFrame, filename: str):
    """Save the pandas dataframe to file"""

    filename = filename.replace(".txt", ".parquet")
    df.to_parquet(filename, index=False)
