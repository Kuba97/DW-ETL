import os

RAW_DATA_DATA_PATH = os.path.join(os.getcwd(), '..', '..', 'DATA_CLEANING', 'raw_data')
ETL_DATA_PATH = os.path.join(RAW_DATA_DATA_PATH, '..', 'processed_data')

GUN_EVENTS_NAME = 'gun-violence.csv'
GUN_STATS_NAME = 'gun_stats.csv'
DEMOGRAPHY_2015_NAME = 'acs2015_county_data.csv'
DEMOGRAPHY_2017_NAME = 'acs2017_county_data.csv'

GUN_EVENTS_TARGET_NAME = 'gun_events.csv'
GUN_PART_TARGET_NAME = 'gun_participant.csv'
GUN_STATS_TARGET_NAME = 'gun_stats.csv'
DEMOGRAPHIC_TARGET_NAME = 'demography.csv'
