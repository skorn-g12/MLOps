'''
filename: utils.py
functions: encode_features, load_model
creator: shashank.gupta
version: 1
'''

###############################################################################
# Import necessary modules
# ##############################################################################

import mlflow
import mlflow.sklearn
import mlflow.pyfunc
import pandas as pd

import sqlite3

import os
import logging
from constants import *
from datetime import datetime


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


###############################################################################
# Define the function to train the model
# ##############################################################################


def encode_features():
    """
    This function one hot encodes the categorical features present in our
    training dataset. This encoding is needed for feeding categorical data
    to many scikit-learn models.

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        ONE_HOT_ENCODED_FEATURES : list of the features that needs to be there in the final encoded dataframe
        FEATURES_TO_ENCODE: list of features  from cleaned data that need to be one-hot encoded
        **NOTE : You can modify the encode_features function used in heart disease's inference
        pipeline for this.

    OUTPUT
        1. Save the encoded features in a table - features

    SAMPLE USAGE
        encode_features()
    """
    conn = get_sql_connection()
    df = pd.read_sql_query("SELECT * FROM model_input", conn)
    encoded_df = pd.DataFrame(columns=ONE_HOT_ENCODED_FEATURES)  # from constants.py
    placeholder_df = pd.DataFrame()
    for f in FEATURES_TO_ENCODE:
        if f in df.columns:
            encoded = pd.get_dummies(df[f])
            encoded = encoded.add_prefix(f + '_')
            placeholder_df = pd.concat([placeholder_df, encoded], axis=1)
        else:
            print('Feature not found')
            return
    encoded_df.drop(FEATURES_TO_ENCODE, axis=1, inplace=True)
    # Implement these steps to prevent dimension mismatch during inference
    for feature in encoded_df.columns:
        if feature in df.columns:
            encoded_df[feature] = df[feature]

    encoded_df = pd.concat([encoded_df, placeholder_df], axis=1)
    # fill all null values
    encoded_df.fillna(0, inplace=True)
    encoded_df.to_sql("features", conn, if_exists="replace", index=False)
    conn.close()


###############################################################################
# Define the function to load the model from mlflow model registry
# ##############################################################################

def get_models_prediction():
    """
    This function loads the model which is in production from mlflow registry and
    uses it to do prediction on the input dataset. Please note this function will the load
    the latest version of the model present in the production stage.

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        model from mlflow model registry
        model name: name of the model to be loaded
        stage: stage from which the model needs to be loaded i.e. production


    OUTPUT
        Store the predicted values along with input data into a table

    SAMPLE USAGE
        load_model()
    """
    mlflow.set_tracking_uri(TRACKING_URI)
    loadedModel = mlflow.pyfunc.load_model(model_uri=MODEL_NAME)
    conn = get_sql_connection()
    X = pd.read_sql_query("SELECT * FROM features", conn)
    yPred = loadedModel.predict(X)

    # Combine the input data and predictions into a single DataFrame
    output_data = pd.concat([X, pd.DataFrame(yPred, columns=['prediction'])], axis=1)
    output_data.to_sql("prediction_results", conn, if_exists="replace", index=False)
    conn.close()  # Store the output data in a table in the database


###############################################################################
# Define the function to check the distribution of output column
# ##############################################################################

def prediction_ratio_check():
    """
    This function calculates the % of 1 and 0 predicted by the model
    and writes it to a file named 'prediction_distribution.txt'.This file
    should be created in the ~/airflow/dags/Lead_scoring_inference_pipeline
    folder.
    This helps us to monitor if there is any drift observed in the predictions
    from our model at an overall level. This would determine our decision on
    when to retrain our model.


    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be

    OUTPUT
        Write the output of the monitoring check in prediction_distribution.txt with
        timestamp.

    SAMPLE USAGE
        prediction_col_check()
    """
    conn = get_sql_connection()
    outputDf = pd.read_sql_query("SELECT * FROM prediction_results", conn)
    totalLength = outputDf["prediction"].count()
    onesCount = outputDf["prediction"].sum()
    zerosCount = totalLength - onesCount

    onesPerct = onesCount / totalLength
    zerosPerct = zerosCount / totalLength

    with open("percentage_output.txt", "w") as file:
        file.write(f'Percentage of 1s: {onesPerct:.2f}%\n')
        file.write(f'Percentage of 0s: {zerosPerct:.2f}%\n')
    conn.close()


###############################################################################
# Define the function to check the columns of input features
# ##############################################################################


def input_features_check():
    """
    This function checks whether all the input columns are present in our new
    data. This ensures the prediction pipeline doesn't break because of change in
    columns in input data.

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        ONE_HOT_ENCODED_FEATURES: List of all the features which need to be present
        in our input data.

    OUTPUT
        It writes the output in a log file based on whether all the columns are present
        or not.
        1. If all the input columns are present then it logs - 'All the models input are present'
        2. Else it logs 'Some of the models inputs are missing'

    SAMPLE USAGE
        input_col_check()
    """
    logging.basicConfig(filename="inference_pipeline.log", level=logging.INFO)
    conn = get_sql_connection()
    X = pd.read_sql_query("SELECT * FROM model_input", conn)
    # Check if all input columns are present
    missing_columns = [col for col in ONE_HOT_ENCODED_FEATURES if col not in X.columns]

    # Log the result
    if not missing_columns:
        logging.info('All the model inputs are present')
    else:
        logging.info('Some of the model inputs are missing')
        logging.info(f'Missing columns: {missing_columns}')


if __name__ == "__main__":
    # Test code here instead of a dummy notebook
    encode_features()
    input_features_check()
    get_models_prediction()
    prediction_ratio_check()
