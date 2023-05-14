##############################################################################
# Import necessary modules and files
##############################################################################

#import sys

# sys.path.append("/run/media/feuer/LinuxDrive/ChromeDownloads/Assignment/01_data_pipeline/notebooks/")
# sys.path.append("/run/media/feuer/LinuxDrive/ChromeDownloads/Assignment/airflow/dags/Lead_scoring_data_pipeline")
import pandas as pd
import os
import sqlite3
from sqlite3 import Error
from constants import *
from mapping.city_tier_mapping import city_tier_mapping
from mapping.significant_categorical_level import *


###############################################################################
# Define the function to build database
###############################################################################

def get_sql_connection():
    """ create a database connection to a SQLite database """
    conn = None
    # opening the connection for creating the sqlite db
    try:
        conn = sqlite3.connect(DB_PATH + "/" + DB_FILE_NAME)
        print(sqlite3.version)
    # return an error if connection not established
    except Error as e:
        print(e)
    finally:
        return conn


def build_dbs():
    '''
    This function checks if the db file with specified name is present 
    in the /Assignment/01_data_pipeline/scripts folder. If it is not present it creates 
    the db file with the given name at the given path. 


    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should exist  


    OUTPUT
    The function returns the following under the given conditions:
        1. If the file exists at the specified path
                prints 'DB Already Exists' and returns 'DB Exists'

        2. If the db file is not present at the specified loction
                prints 'Creating Database' and creates the sqlite db 
                file at the specified path with the specified name and 
                once the db file is created prints 'New DB Created' and 
                returns 'DB created'


    SAMPLE USAGE
        build_dbs()
    '''
    returnStatus = "DB Exists"
    if os.path.isfile(DB_PATH + "/" + DB_FILE_NAME):
        print("DB Already Exists")
    else:
        print("Creating Database")
        with open(DB_PATH + "/" + DB_FILE_NAME, "w") as fp:
            pass
        returnStatus = "DB created"

    conn = get_sql_connection()
    conn.close()


###############################################################################
# Define function to load the csv file to the database
###############################################################################

def load_data_into_db(dataDir=DATA_DIRECTORY, csvName=RAW_CSV_NAME):
    '''
    This function loads the data present in data directory into the db
    which was created previously.
    It also replaces any null values present in 'total_leads_dropped' and
    'referred_lead' columns with 0.


    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be
        DATA_DIRECTORY : path of the directory where 'leadscoring.csv' 
                        file is present
        

    OUTPUT
        Saves the processed dataframe in the db in a table named 'loaded_data'.
        If the table with the same name already exists then the function
        replaces it.


    SAMPLE USAGE
        load_data_into_db()
    '''
    df = pd.read_csv(f"{dataDir}/{csvName}", index_col=0)
    df.rename(columns={"total_leads_droppped": "total_leads_dropped"}, inplace=True)
    if "total_leads_dropped" in df.columns:
        df["total_leads_dropped"].fillna(0, inplace=True)
    if "referred_lead" in df.columns:
        df["referred_lead"].fillna(0, inplace=True)

    conn = get_sql_connection()
    df.to_sql("loaded_data", conn, if_exists="replace", index=False)
    conn.close()


###############################################################################
# Define function to map cities to their respective tiers
###############################################################################


def map_city_tier():
    '''
    This function maps all the cities to their respective tier as per the
    mappings provided in the city_tier_mapping.py file. If a
    particular city's tier isn't mapped(present) in the city_tier_mapping.py 
    file then the function maps that particular city to 3.0 which represents
    tier-3.


    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be
        city_tier_mapping : a dictionary that maps the cities to their tier

    
    OUTPUT
        Saves the processed dataframe in the db in a table named
        'city_tier_mapped'. If the table with the same name already 
        exsists then the function replaces it.

    
    SAMPLE USAGE
        map_city_tier()

    '''
    conn = get_sql_connection()
    # dfLeadScoring = pd.read_sql_table("loaded_data", conn)
    dfLeadScoring = pd.read_sql_query("SELECT * FROM loaded_data", conn)
    dfLeadScoring["city_tier"] = dfLeadScoring["city_mapped"].map(city_tier_mapping)
    dfLeadScoring["city_tier"] = dfLeadScoring["city_tier"].fillna(3.0)
    dfLeadScoring = dfLeadScoring.drop(['city_mapped'], axis=1)
    # df = preprocessCityTier(dfLeadScoring)
    dfLeadScoring.to_sql("city_tier_mapped", conn, if_exists="replace", index=False)
    # Close connection once all is done
    conn.close()


###############################################################################
# Define function to map insignificant categorial variables to "others"
###############################################################################

def preprocessCategoricalVars(dfLeadScoring):
    # all the levels below 90 percentage are assgined to a single level called others
    new_df = dfLeadScoring[~dfLeadScoring['first_platform_c'].isin(
        list_platform)]  # get rows for levels which are not present in list_platform
    new_df['first_platform_c'] = "others"  # replace the value of these levels to others
    old_df = dfLeadScoring[dfLeadScoring['first_platform_c'].isin(
        list_platform)]  # get rows for levels which are present in list_platform
    df = pd.concat([new_df, old_df])  # concatenate new_df and old_df to get the final dataframe

    # all the levels below 90 percentage are assgined to a single level called others
    new_df = df[~df['first_utm_medium_c'].isin(list_medium)]  # get rows for levels which are not present in list_medium
    new_df['first_utm_medium_c'] = "others"  # replace the value of these levels to others
    old_df = df[df['first_utm_medium_c'].isin(list_medium)]  # get rows for levels which are present in list_medium
    df = pd.concat([new_df, old_df])  # concatenate new_df and old_df to get the final dataframe

    # all the levels below 90 percentage are assgined to a single level called others
    new_df = df[~df['first_utm_source_c'].isin(list_source)]  # get rows for levels which are not present in list_source
    new_df['first_utm_source_c'] = "others"  # replace the value of these levels to others
    old_df = df[df['first_utm_source_c'].isin(list_source)]  # get rows for levels which are present in list_source
    df = pd.concat([new_df, old_df])  # concatenate new_df and old_df to get the final dataframe
    return df


def map_categorical_vars():
    '''
    This function maps all the insignificant variables present in 'first_platform_c'
    'first_utm_medium_c' and 'first_utm_source_c'. The list of significant variables
    should be stored in a python file in the 'significant_categorical_level.py' 
    so that it can be imported as a variable in utils file.
    

    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be present
        list_platform : list of all the significant platform.
        list_medium : list of all the significat medium
        list_source : list of all rhe significant source

        **NOTE : list_platform, list_medium & list_source are all constants and
                 must be stored in 'significant_categorical_level.py'
                 file. The significant levels are calculated by taking top 90
                 percentils of all the levels. For more information refer
                 'data_cleaning.ipynb' notebook.
  

    OUTPUT
        Saves the processed dataframe in the db in a table named
        'categorical_variables_mapped'. If the table with the same name already 
        exists then the function replaces it.

    SAMPLE USAGE
        map_categorical_vars()
    '''
    conn = get_sql_connection()
    # dfLeadScoring = pd.read_sql_table("loaded_data", conn)
    dfLeadScoring = pd.read_sql_query("SELECT * FROM city_tier_mapped", conn)
    df = preprocessCategoricalVars(dfLeadScoring)
    df = df.drop_duplicates()
    df.to_sql("categorical_variables_mapped", conn, if_exists="replace", index=False)
    # Close connection once all is done
    conn.close()


##############################################################################
# Define function that maps interaction columns into 4 types of interactions
##############################################################################
def interactions_mapping():
    '''
    This function maps the interaction columns into 4 unique interaction columns
    These mappings are present in 'interaction_mapping.csv' file. 


    INPUTS
        DB_FILE_NAME: Name of the database file
        DB_PATH : path where the db file should be present
        INTERACTION_MAPPING : path to the csv file containing interaction's
                                   mappings
        INDEX_COLUMNS_TRAINING : list of columns to be used as index while pivoting and
                                 unpivoting during training
        INDEX_COLUMNS_INFERENCE: list of columns to be used as index while pivoting and
                                 unpivoting during inference
        NOT_FEATURES: Features which have less significance and needs to be dropped
                                 
        NOTE : Since while inference we will not have 'app_complete_flag' which is
        our label, we will have to exclude it from our features list. It is recommended
        that you use an if loop and check if 'app_complete_flag' is present in 
        'categorical_variables_mapped' table and if it is present pass a list with 
        'app_complete_flag' column, or else pass a list without 'app_complete_flag'
        column.


    OUTPUT
        Saves the processed dataframe in the db in a table named 
        'interactions_mapped'. If the table with the same name already exists then
        the function replaces it.
        
        It also drops all the features that are not required for training model and
        writes it in a table named 'model_input'

    
    SAMPLE USAGE
        interactions_mapping()
    '''
    # read the interaction mapping file
    df_event_mapping = pd.read_csv(f"{INTERACTION_MAPPING}", index_col=[0])
    conn = get_sql_connection()
    # dfLeadScoring = pd.read_sql_table("loaded_data", conn)
    df = pd.read_sql_query("SELECT * FROM categorical_variables_mapped", conn)
    df_unpivot = pd.melt(df, id_vars=INDEX_COLUMNS_TRAINING,
                         var_name="interaction_type",
                         value_name="interaction_value")
    # handle the nulls in the interaction value column
    df_unpivot['interaction_value'] = df_unpivot['interaction_value'].fillna(0)
    # map interaction type column with the mapping file to get interaction mapping
    df = pd.merge(df_unpivot, df_event_mapping, on='interaction_type', how='left')
    # dropping the interaction type column as it is not needed
    df = df.drop(['interaction_type'], axis=1)
    # pivoting the interaction mapping column values to individual columns in the dataset
    df_pivot = df.pivot_table(values='interaction_value',
                              index=INDEX_COLUMNS_TRAINING,
                              columns='interaction_mapping',
                              aggfunc='sum')
    df_pivot = df_pivot.reset_index()
    df_pivot.to_sql("interactions_mapping", conn, if_exists="replace", index=False)
    dfModelInput = pd.DataFrame()
    if "app_complete_flag" in df_pivot.columns:
        dfModelInput = df_pivot[TRAINING_FEATURES]
    else:
        dfModelInput = df_pivot[INFERENCE_FEATURES]
    dfModelInput.to_sql("model_input", conn, if_exists="replace", index=False)
    # Close connection once all is done
    conn.close()


if __name__ == "__main__":
    # build_dbs()
    # load_data_into_db()
    # map_city_tier()
    # map_categorical_vars()
    interactions_mapping()
