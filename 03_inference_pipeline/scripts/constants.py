DB_PATH = "/run/media/feuer/LinuxDrive/ChromeDownloads/Assignment/airflow/dags/Lead_scoring_inference_pipeline"
DB_FILE_NAME = "lead_scoring_inference.db"

DB_FILE_MLFLOW = ""

FILE_PATH = ""

TRACKING_URI = "http://0.0.0.0:6006"

# experiment, model name and stage to load the model from mlflow model registry
MODEL_NAME = "models:/LGBM_ImportantFeatures/production"
STAGE = "Production"
MLFLOW_EXPERIMENT_NAME = "Inference"

# list of the features that needs to be there in the final encoded dataframe
ONE_HOT_ENCODED_FEATURES = ['total_leads_dropped', 'city_tier', 'referred_lead',
                            'first_platform_c', 'first_utm_medium_c', 'first_utm_source_c']

# list of features that need to be one-hot encoded
FEATURES_TO_ENCODE = ["city_tier", "first_platform_c", 'first_utm_medium_c', 'first_utm_source_c']
