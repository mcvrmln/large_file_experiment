""" 
Snowpark demo by Snowflake

I altered this demo. The table used can be found in the SNOWFLAKE_SAMPLE_DATA.
"""

import yaml

from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col


def read_config():
    """Reads the config file (hardcoded ./config.yml)"""

    with open("./config/config.yml", "r") as conf:
        configuration = yaml.safe_load(conf)

    return configuration


# Create Session object
def create_session_object(connection_parameters: dict):
    """
    Demo function provided by Snowflake
    Altered to accept a dictionary (from yaml file)
    """

    session = Session.builder.configs(connection_parameters).create()

    return session


def create_dataframe(session):

    # Create a dataframe
    df_table = session.table("CUSTOMER")

    # ---------------------------------
    # **ACTIONS**
    # ---------------------------------

    # count method
    df_table.count()
    # print(df_table.count())

    # show method
    df_table.show()

    # collect method
    df_results = df_table.collect()
    # print(df_results)

    # ---------------------------------
    # **TRANSFORMATIONS **
    # ---------------------------------

    df_filtered = df_table.filter(col("C_CUSTKEY") == 60001)

    # Chaining method calls
    # df_filtered = df_table.filter(col("AGE") > 30).sort(col("AGE").desc()).limit(10)

    df_filtered.show()

    df_filtered.collect()

    df_filtered_persisted = df_filtered.collect()
    # print(df_filtered_persisted)


# ------------------------------------------------------------------------------------------------------------------

# FUNCTION CALLS


# Function call
configuration = read_config()
# call session object
session = create_session_object(configuration["admin"])

# call create dataframe
_ = create_dataframe(session)

# end your session
session.close()
