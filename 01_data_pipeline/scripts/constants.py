# You can create more variables according to your project. The following are the basic variables that have been
# provided to you
DB_PATH = '/run/media/feuer/LinuxDrive/ChromeDownloads/Assignment/01_data_pipeline/scripts'
DB_FILE_NAME = 'utils_output.db'
UNIT_TEST_DB_FILE_NAME = ''
DATA_DIRECTORY = '/run/media/feuer/LinuxDrive/ChromeDownloads/Assignment/01_data_pipeline/notebooks/Data/'
RAW_CSV_NAME = "leadscoring.csv"
INTERACTION_MAPPING = '/run/media/feuer/LinuxDrive/ChromeDownloads/Assignment/01_data_pipeline/scripts/interaction_mapping.csv'
INDEX_COLUMNS_TRAINING = ['created_date', 'city_tier', 'first_platform_c',
                          'first_utm_medium_c', 'first_utm_source_c', 'total_leads_dropped',
                          'referred_lead', 'app_complete_flag']
INDEX_COLUMNS_INFERENCE = ['created_date', 'city_tier', 'first_platform_c',
                           'first_utm_medium_c', 'first_utm_source_c', 'total_leads_dropped',
                           'referred_lead']
NOT_FEATURES = []
