"""
Import necessary modules
############################################################################## 
"""

import pandas as pd
import sys
from constants import *
from schema import *
from utils import *


###############################################################################
# Define function to validate raw data's schema
############################################################################### 

def raw_data_schema_check():
    '''
    This function check if all the columns mentioned in schema.py are present in
    leadscoring.csv file or not.

   
    INPUTS
        DATA_DIRECTORY : path of the directory where 'leadscoring.csv' 
                        file is present
        raw_data_schema : schema of raw data in the form oa list/tuple as present 
                          in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Raw datas schema is in line with the schema present in schema.py' 
        else prints
        'Raw datas schema is NOT in line with the schema present in schema.py'

    
    SAMPLE USAGE
        raw_data_schema_check
    '''
    df = pd.read_csv(f"{DATA_DIRECTORY}/leadscoring.csv", index_col=0)
    if set(df.columns) == set(raw_data_schema):
        print("Raw data's schema is in line with the schema present in schema.py")
    else:
        print("Raw data's schema is NOT in line with the schema present in schema.py")


###############################################################################
# Define function to validate model's input schema
############################################################################### 

def model_input_schema_check():
    '''
    This function check if all the columns mentioned in model_input_schema in 
    schema.py are present in table named in 'model_input' in db file.

   
    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be present
        model_input_schema : schema of models input data in the form oa list/tuple
                          present as in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Models input schema is in line with the schema present in schema.py'
        else prints
        'Models input schema is NOT in line with the schema present in schema.py'
    
    SAMPLE USAGE
        raw_data_schema_check
    '''
    conn = get_sql_connection()
    dfFinal = pd.read_sql_query("SELECT * FROM model_input", conn)
    if set(dfFinal.columns) == set(model_input_schema):
        print("Models input schema is in line with the schema present in schema.py")
    else:
        print("Models input schema is NOT in line with the schema present in schema.py")


if __name__ == "__main__":
    build_dbs()
    raw_data_schema_check()
    load_data_into_db()
    map_city_tier()
    map_categorical_vars()
    interactions_mapping()
    model_input_schema_check()
