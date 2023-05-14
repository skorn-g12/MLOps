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
            return df

    # Implement these steps to prevent dimension mismatch during inference
    for feature in encoded_df.columns:
        if feature in df.columns:
            encoded_df[feature] = df[feature]
        if feature in placeholder_df.columns:
            encoded_df[feature] = placeholder_df[feature]
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
    '''
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
    '''
    mlflow.set_tracking_uri(TRACKING_URI)
    mlflow.set_experiment("Lead_Scoring_Training_Pipeline")
    gridParams = {
        'learning_rate': [0.005, 0.01, 0.05, 0.1],
        'n_estimators': [8, 16, 24, 32],
        'num_leaves': [6, 8, 12, 16],  # large num_leaves helps improve accuracy but might lead to over-fitting
        'boosting_type': ['gbdt', 'dart'],  # for better accuracy -> try dart
        'objective': ['binary'],
        'max_bin': [255, 510],  # large max_bin helps improve accuracy but might slow down training progress
        'random_state': [500],
        'colsample_bytree': [0.64, 0.65, 0.66],
        'subsample': [0.7, 0.75],
        'reg_alpha': [1, 1.2],
        'reg_lambda': [1, 1.2, 1.4],
    }
    conn = get_sql_connection()
    X = pd.read_sql_query("SELECT * FROM features", conn)
    y = pd.read_sql_query("SELECT * FROM target", conn)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    # Defining LightGBM model and GridSearchCV object
    model = lgb.LGBMClassifier(random_state=42, objective='binary')
    grid = GridSearchCV(estimator=model, param_grid=gridParams, cv=3, n_jobs=-1, scoring='roc_auc')
    # Training the model with GridSearchCV
    with mlflow.start_run() as run:
        grid.fit(X_train, y_train)
        # Get auc score on test data
        model = grid.best_estimator_
        y_pred = model.predict(X_test)
        aucScore = roc_auc_score(y_test, y_pred)

        # Logging best params, score and model to MLflow
        mlflow.log_metric("AUC_Test", aucScore)
        mlflow.log_params(grid.best_params_)
        mlflow.log_metric('auc_train', grid.best_score_)
        mlflow.lightgbm.log_model(grid.best_estimator_, 'lgbm_model')


if __name__ == "__main__":
    encode_features()
    get_trained_model()
