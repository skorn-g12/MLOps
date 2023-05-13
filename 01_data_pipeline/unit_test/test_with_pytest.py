##############################################################################
# Import the necessary modules
##############################################################################
import sys

sys.path.append("/run/media/feuer/LinuxDrive/ChromeDownloads/Assignment/01_data_pipeline/notebooks")
import pandas as pd
import os
import sqlite3
from sqlite3 import Error
from constants import *
from Maps.city_tier import city_tier_mapping
from significant_categorical_level import *
from utils import *


###############################################################################
# Write test cases for load_data_into_db() function
# ##############################################################################
def get_test_sql_connection():
    """ create a database connection to a SQLite database """
    conn = None
    # opening the connection for creating the sqlite db
    try:
        conn = sqlite3.connect("unit_test_cases.db")
        print(sqlite3.version)
    # return an error if connection not established
    except Error as e:
        print(e)
    finally:
        return conn


def test_load_data_into_db():
    """_summary_
    This function checks if the load_data_into_db function is working properly by
    comparing its output with test cases provided in the db in a table named
    'loaded_data_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'

    SAMPLE USAGE
        output=test_get_data()
    """
    # 1. Establish connection to unit_test_cases.db
    testConn = get_test_sql_connection()
    # 2. Get df from this db
    dfGroundTruth = pd.read_sql_query("SELECT * FROM loaded_data_test_case", testConn)

    # 3. Run load_data_into_db, making sure that the csv that is going to be written to the table "loaded_data" is
    # "leadscoring_test.csv"
    load_data_into_db(dataDir="/run/media/feuer/LinuxDrive/ChromeDownloads/Assignment/01_data_pipeline/unit_test",
                      csvName="leadscoring_test.csv")
    # 4. Establish connection to myOutput.db, which is the db where all the preprocessing stages' outputs will be saved
    conn = get_sql_connection()

    # 5. Obtain the dataframe from the above db
    dfToCheck = pd.read_sql_query("SELECT * FROM loaded_data", conn)

    # 6. Ensure that ground truth obtained from unit_test_cases.db matches load_data_into_db()
    assert (dfToCheck.equals(dfGroundTruth)), "load_data_into_db does not match ground truth"


###############################################################################
# Write test cases for map_city_tier() function
# ##############################################################################
def test_map_city_tier():
    """_summary_
    This function checks if map_city_tier function is working properly by
    comparing its output with test cases provided in the db in a table named
    'city_tier_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'

    SAMPLE USAGE
        output=test_map_city_tier()

    """
    # 1. Establish connection to unit_test_cases.db
    testConn = get_test_sql_connection()
    # 2. Get df from this db
    dfGroundTruth = pd.read_sql_query("SELECT * FROM city_tier_mapped_test_case", testConn)

    # 3. Run map_city_tier() which will write the final df to the table: city_tier_mapped in the db
    map_city_tier()

    # 4. Now output final df and match against ground truth
    conn = get_sql_connection()
    dfToCheck = pd.read_sql_query("SELECT * FROM city_tier_mapped", conn)
    assert (dfToCheck.equals(dfGroundTruth)), "map_city_tier does not match ground truth"


###############################################################################
# Write test cases for map_categorical_vars() function
# ##############################################################################    
def test_map_categorical_vars():
    """_summary_
    This function checks if map_cat_vars function is working properly by
    comparing its output with test cases provided in the db in a table named
    'categorical_variables_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'
    
    SAMPLE USAGE
        output=test_map_cat_vars()

    """
    # 1. Establish connection to unit_test_cases.db
    testConn = get_test_sql_connection()
    # 2. Get df from this db
    dfGroundTruth = pd.read_sql_query("SELECT * FROM categorical_variables_mapped_test_case", testConn)
    # 3. Run map_categorical_vars() which will write the final df to the table: categorical_variables_mapped in the db
    map_categorical_vars()

    # 4. Now output final df and match against ground truth
    conn = get_sql_connection()
    dfToCheck = pd.read_sql_query("SELECT * FROM categorical_variables_mapped", conn)
    assert (dfToCheck.equals(dfGroundTruth)), "map_categorical_vars does not match ground truth"


###############################################################################
# Write test cases for interactions_mapping() function
# ##############################################################################    
def test_interactions_mapping():
    """_summary_
    This function checks if test_column_mapping function is working properly by
    comparing its output with test cases provided in the db in a table named
    'interactions_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'

    SAMPLE USAGE
        output=test_column_mapping()

    """
    # 1. Establish connection to unit_test_cases.db
    testConn = get_test_sql_connection()
    # 2. Get df from this db
    dfGroundTruth = pd.read_sql_query("SELECT * FROM interactions_mapped_test_case", testConn)
    # 3. Run map_categorical_vars() which will write the final df to the table: interactions_mapping in the db
    interactions_mapping()

    # 4. Now output final df and match against ground truth
    conn = get_sql_connection()
    dfToCheck = pd.read_sql_query("SELECT * FROM interactions_mapping", conn)
    assert (dfToCheck.equals(dfGroundTruth)), "interactions_mapping does not match ground truth"


if __name__ == "__main__":
    test_map_categorical_vars()
    test_interactions_mapping()
