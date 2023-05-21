DB_PATH = "/run/media/feuer/LinuxDrive/ChromeDownloads/Assignment/airflow/dags/Lead_scoring_training_pipeline"

DB_FILE_NAME = "lead_scoring_training.db"

TRACKING_URI = "http://0.0.0.0:6006"
EXPERIMENT = "ModelExperimentation"

# model config imported from pycaret experimentation

MODEL_CFG = {"n_jobs": -1,
             "log_experiment": True,
             "experiment_name": MLFLOW_EXPERIMENT_NAME,
             "log_plots": True,
             "log_data": True,
             "verbose": True,
             "log_profile": False}

# list of the features that needs to be there in the final encoded dataframe
ONE_HOT_ENCODED_FEATURES = ['total_leads_dropped', 'city_tier', 'referred_lead', 'app_complete_flag',
                            'first_platform_c', 'first_utm_medium_c', 'first_utm_source_c']

GRID_PARAMS = {
    'learning_rate': [0.005, 0.01, 0.05, 0.1],
    'n_estimators': [8,16,24, 32],
    'num_leaves': [6,8,12,16], # large num_leaves helps improve accuracy but might lead to over-fitting
    'boosting_type' : ['gbdt', 'dart'], # for better accuracy -> try dart
    'objective' : ['binary'],
    'max_bin':[255, 510], # large max_bin helps improve accuracy but might slow down training progress
    'random_state' : [500],
    'colsample_bytree' : [0.64, 0.65, 0.66],
    'subsample' : [0.7,0.75],
    'reg_alpha' : [1,1.2],
    'reg_lambda' : [1,1.2,1.4],
    }
# list of features that need to be one-hot encoded
FEATURES_TO_ENCODE = ["city_tier", "first_platform_c", 'first_utm_medium_c', 'first_utm_source_c']
EXCLUDE_MODELS = ['gbc', 'knn', 'qda', 'dummy', 'svm', 'ada', 'xgboost']
