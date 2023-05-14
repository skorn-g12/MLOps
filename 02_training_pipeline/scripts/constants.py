DB_PATH = "/run/media/feuer/LinuxDrive/ChromeDownloads/Assignment/02_training_pipeline/scripts"

DB_FILE_NAME = "lead_scoring_training.db"

TRACKING_URI = "http://0.0.0.0:6008"
EXPERIMENT = "ModelExperimentation"

# model config imported from pycaret experimentation
model_config = ""

# list of the features that needs to be there in the final encoded dataframe
ONE_HOT_ENCODED_FEATURES = ['total_leads_dropped', 'city_tier', 'referred_lead', 'app_complete_flag',
                            'first_platform_c', 'first_utm_medium_c', 'first_utm_source_c']
# list of features that need to be one-hot encoded
FEATURES_TO_ENCODE = ["city_tier", "first_platform_c", 'first_utm_medium_c', 'first_utm_source_c']
