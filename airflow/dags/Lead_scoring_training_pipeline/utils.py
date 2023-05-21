'''
filename: utils.py
functions: encode_features, get_train_model
creator: shashank.gupta
version: 1
'''

###############################################################################
# Import necessary modules
# ##############################################################################

import pandas as pd
import numpy as np

import sqlite3
from sqlite3 import Error

import mlflow
import mlflow.sklearn

from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
import lightgbm as lgb
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import accuracy_score

from constants import *
import mlflow
from pycaret.classification import *


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
# Define the function to encode features
# ##############################################################################

def encode_features():
    '''
    This function one hot encodes the categorical features present in our  
    training dataset. This encoding is needed for feeding categorical data 
    to many scikit-learn models.

    INPUTS
        db_file_name : Name of the database file 
        db_path : path where the db file should be
        ONE_HOT_ENCODED_FEATURES : list of the features that needs to be there in the final encoded dataframe
        FEATURES_TO_ENCODE: list of features  from cleaned data that need to be one-hot encoded
       

    OUTPUT
        1. Save the encoded features in a table - features
        2. Save the target variable in a separate table - target


    SAMPLE USAGE
        encode_features()
        
    **NOTE : You can modify the encode_featues function used in heart disease's inference
        pipeline from the pre-requisite module for this.
    '''
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
    target = encoded_df.pop("app_complete_flag")
    encoded_df.to_sql("features", conn, if_exists="replace", index=False)
    target.to_sql("target", conn, if_exists="replace", index=False)
    conn.close()


###############################################################################
# Define the function to train the model
# ##############################################################################

def get_trained_model():
    """
    This function setups mlflow experiment to track the run of the training pipeline. It
    also trains the model based on the features created in the previous function and
    logs the train model into mlflow model registry for prediction. The input dataset is split
    into train and test data and the auc score calculated on the test data and
    recorded as a metric in mlflow run.

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be


    OUTPUT
        Tracks the run in experiment named 'Lead_Scoring_Training_Pipeline'
        Logs the trained model into mlflow model registry with name 'LightGBM'
        Logs the metrics and parameters into mlflow run
        Calculate auc from the test data and log into mlflow run  

    SAMPLE USAGE
        get_trained_model()
    """
    mlflow.set_tracking_uri(TRACKING_URI)
    # mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)
    conn = get_sql_connection()
    X = pd.read_sql_query("SELECT * FROM features", conn)
    y = pd.read_sql_query("SELECT * FROM target", conn)
    dfAllFeatures = pd.concat([X, y], axis=1)
    # Split the DataFrame into train and test sets
    dfTrain, dfTest = train_test_split(dfAllFeatures, test_size=0.2, random_state=42)

    with mlflow.start_run() as run:
        # All features' expt
        baseline_allFeaturesExpt = setup(data=dfTrain,
                                         target='app_complete_flag',
                                         test_data=dfTest,
                                         session_id=42,
                                         fix_imbalance=False,
                                         **MODEL_CFG)

        bestModel = compare_models(exclude=EXCLUDE_MODELS, sort="AUC")

        # Logging best params, score and model to MLflow

        # Get the train and test AUC scores
        y_train = dfTrain.pop("app_complete_flag")
        y_test = dfTest.pop("app_complete_flag")
        X_train = dfTrain
        X_test = dfTest
        # Tuning
        tuned_lgbm = tune_model(bestModel,
                                optimize="AUC",
                                fold=5,
                                n_iter=50,
                                verbose=False,
                                custom_grid=GRID_PARAMS,
                                search_library="optuna")

        # Logging in mlflow
        yPredTest = tuned_lgbm.predict(X_test)
        yPredTrain = tuned_lgbm.predict(X_train)
        test_auc = roc_auc_score(y_test, yPredTest)
        train_auc = roc_auc_score(y_train, yPredTrain)
        mlflow.log_metric("AUC_Train", train_auc)
        mlflow.log_metric("AUC_Test", test_auc)
        mlflow.log_params(tuned_lgbm.get_params())
        mlflow.lightgbm.log_model(tuned_lgbm, 'Best_estimator')


if __name__ == "__main__":
    encode_features()
    get_trained_model()
