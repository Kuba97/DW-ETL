import os
import warnings

from utils.data_io import load_data, store_data
from utils.util import print_progress_bar
from config import (RAW_DATA_DATA_PATH, ETL_DATA_PATH, DEMOGRAPHY_2015_NAME, DEMOGRAPHY_2017_NAME, GUN_EVENTS_NAME,
                    GUN_STATS_NAME, DEMOGRAPHIC_TARGET_NAME, GUN_EVENTS_TARGET_NAME, GUN_PART_TARGET_NAME,
                    GUN_STATS_TARGET_NAME)
from preprocessing.clean_gun_events import clean_gun_events
from preprocessing.clean_demographic import clean_demographic
from preprocessing.clean_gun_stats import clean_gun_stats
from integration.integrate_data import integrate_data


def process_etl():
    gun_events, gun_stats, demograpy_2015, demograpy_2017 = load_raw_data()
    print_progress_bar(1, 7)

    # preprocessing stage
    gun_events, participants = clean_gun_events(gun_events)
    print_progress_bar(2, 7)
    demographic = clean_demographic(demograpy_2015, demograpy_2017)
    print_progress_bar(3, 7)
    gun_stats = clean_gun_stats(gun_stats)
    print_progress_bar(4, 7)

    # integration stage
    gun_events, participants, gun_stats, demographic = integrate_data(gun_events, participants, gun_stats, demographic)
    print_progress_bar(6, 7)
    store_processed_data(gun_events, participants, gun_stats, demographic)
    print_progress_bar(7, 7)


def load_raw_data():
    gun_events = load_data(os.path.join(RAW_DATA_DATA_PATH, GUN_EVENTS_NAME))
    gun_stats = load_data(os.path.join(RAW_DATA_DATA_PATH, GUN_STATS_NAME))
    demography_2015 = load_data(os.path.join(RAW_DATA_DATA_PATH, DEMOGRAPHY_2015_NAME))
    demography_2017 = load_data(os.path.join(RAW_DATA_DATA_PATH, DEMOGRAPHY_2017_NAME))
    return gun_events, gun_stats, demography_2015, demography_2017


def store_processed_data(gun_events, participants, gun_stats, demographic):
    store_data(gun_events, os.path.join(ETL_DATA_PATH, GUN_EVENTS_TARGET_NAME))
    store_data(participants, os.path.join(ETL_DATA_PATH, GUN_PART_TARGET_NAME))
    store_data(gun_stats, os.path.join(ETL_DATA_PATH, GUN_STATS_TARGET_NAME))
    store_data(demographic, os.path.join(ETL_DATA_PATH, DEMOGRAPHIC_TARGET_NAME))


if __name__ == '__main__':
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        print('Executing ETL process on data...')
        process_etl()
        print('ETL process finished - check target path for data')
