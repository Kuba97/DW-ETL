import pandas as pd
from utils.data_funcs import select_attributes, cast_to_int

GUN_STATS_ATTR_TO_INT = ['StateID', 'Totals', 'Handgun', 'LongGun', 'Other', 'Multiple']
DEMOGRAPHY_ATTR_TO_INT = ['StateID', 'CityID', 'Year', 'TotalPop', 'Men', 'Women', 'Income', 'Citizen', 'Employed']
GUN_EVENTS_ATTR_TO_INT = ['StateID', 'CityID', 'Killed', 'Injured', 'Guns', 'CityID']
GUN_PART_ATTR_TO_INT = ['Age']


def integrate_data(gun_events, gun_part, gun_stats, demography):
    """
    Executes integration pipeline in all data sets
    :param gun_events: pd.DataFrame with data about gun violence events
    :param gun_part: pd.DataFrame with data about gun violence events' participants
    :param gun_stats: pd.DataFrame with data about gun possession statistics
    :param demography: pd.DataFrame with data about demographic conditions
    :return: tuple of pd.DataFrames with integrated data
    """
    standardise_attr_names(gun_events, gun_part, gun_stats, demography)
    gun_events, demography = standardize_city_names(gun_events, demography)
    gun_events = delete_extra_city_annotation(gun_events)
    states, cities, states_cities = create_state_cities_table(gun_events, demography)
    gun_events, demography, gun_stats = map_to_state_city_id(gun_events, demography, gun_stats, states_cities, cities,
                                                             states)
    gun_stats = cast_to_int(gun_stats, GUN_STATS_ATTR_TO_INT)
    demography = cast_to_int(demography, DEMOGRAPHY_ATTR_TO_INT)
    gun_events = cast_to_int(gun_events, GUN_EVENTS_ATTR_TO_INT)
    gun_part = cast_to_int(gun_part, GUN_PART_ATTR_TO_INT)

    return gun_events, gun_part, gun_stats, demography


def standardise_attr_names(gun_events, gun_part, gun_stats, demography):
    """
    Performs attributes' name standardization
    :param gun_events: pd.DataFrame with data about gun violence events
    :param gun_part: pd.DataFrame with data about gun violence events' participants
    :param gun_stats: pd.DataFrame with data about gun possession statistics
    :param demography: pd.DataFrame with data about demographic conditions
    :return: None
    """
    gun_events.columns = ['Date', 'State', 'City', 'Killed', 'Injured', 'Guns']
    gun_events.index.name = 'ID'

    gun_part.columns = ['Age', 'Gender', 'Type', 'Arrested', 'Injured', 'Killed', 'Unharmed']
    gun_part.index.names = ['EventID', 'ParticipantID']

    gun_stats.columns = ['State', 'Month', 'Permit', 'Totals', 'Handgun',
                         'LongGun', 'Other', 'Multiple']
    gun_stats.index.name = 'ID'

    demography.columns = ['Year', 'State', 'City', 'TotalPop', 'Men', 'Women',
                          'Hispanic', 'White', 'Black', 'Native', 'Asian', 'Pacific', 'Citizen',
                          'Income', 'Poverty', 'ChildPoverty', 'Employed', 'Unemployment']
    demography.index.name = 'ID'


def standardize_city_names(gun_events, demography):
    """
    Standardizes city names to be most likely to matching across two DataFrames
    :param gun_events: pd.DataFrame
    :param demography: pd.DataFrame
    :return: tuple of pd.DataFrames
    """
    gun_events['City'] = gun_events['City'].str.lower().str.capitalize()
    demography['City'] = demography['City'].str.lower().str.capitalize()
    return gun_events, demography


def delete_extra_city_annotation(gun_events):
    """
    Deletes extra annotations from city names, e.g. New York (Manhattan) -> New York to match names from demography data
    :param gun_events: pd.DataFrame
    :return: pd.DataFrame with cleaned city names
    """
    gun_events['City'] = gun_events['City'].str.split(r"\s*\(").str[0]
    return gun_events


def create_state_cities_table(gun_events, demography):
    """
    Creating tables with states, cities and states_cities
    :return: tuple of pd.DataFrame with states, cities, state_cities
    """
    states_cities = pd.concat([gun_events[['State', 'City']], demography[['State', 'City']]]).drop_duplicates(
        subset=['State', 'City']).reset_index(drop=True)
    states_cities.columns = ['State', 'City']
    states = pd.DataFrame({'State': states_cities['State'].unique()})
    states.index.name = 'ID'
    cities = pd.DataFrame({'City': states_cities['City'].unique()})
    states.index.name = 'ID'
    states_cities = \
        states_cities.merge(right=cities.reset_index().rename(columns={'index': 'CityID'}), on='City', how='right') \
            .merge(right=states.reset_index().rename(columns={'index': 'StateID'}), on='State', how='right') \
            .rename({'ID_x': 'CityID', 'ID': 'StateID'}, axis=1)[['CityID', 'StateID']]
    return states, cities, states_cities


def map_to_state_city_id(gun_events, demography, gun_stats, states_cities, cities, states):
    """
    Maps columns in data sets into referencing indeces from separate tables about states and cities
    :param gun_events: pd.DataFrame
    :param demography: pd.DataFrame
    :param gun_stats: pd.DataFrame
    :param states_cities: pd.DataFrame
    :param cities: pd.DataFrame
    :param states: pd.DataFrame
    :return: tuple of pd.DataFrame with mapped values in tables gun_events, demography, gun_stats
    """
    states_cities_joined = states_cities.join(other=cities, on='CityID').join(other=states, on='StateID')

    # mapping gun_events and changing column order with new created columns
    gun_events = gun_events.merge(states_cities_joined, on=['State', 'City'], how='inner').drop(['City', 'State'],
                                                                                                axis=1)
    gun_events.index.name = 'ID'
    gun_events_new_col_order = ['StateID', 'CityID', 'Date', 'Killed', 'Injured', 'Guns']
    gun_events = gun_events[gun_events_new_col_order]

    # mapping demography and changing column order with new created columns
    demography = demography.merge(states_cities_joined, on=['State', 'City'], how='inner').drop(['City', 'State'],
                                                                                                axis=1)
    demography_new_col_order = ['StateID', 'CityID', 'Year', 'TotalPop', 'Men', 'Women', 'Hispanic', 'White', 'Black',
                                'Native', 'Asian', 'Pacific', 'Citizen', 'Income', 'Poverty', 'ChildPoverty',
                                'Employed', 'Unemployment']

    demography = demography[demography_new_col_order]

    # mapping gun_stats and changing column order with new created columns
    gun_stats = gun_stats.merge(right=states.reset_index().rename({'index': 'StateID'}), on='State',
                                how='right').rename({'ID': 'StateID'}, axis=1).drop(['State'], axis=1)
    gun_stats_new_col_order = ['StateID', 'Month', 'Totals', 'Handgun', 'LongGun', 'Other',
                               'Multiple']
    gun_stats = gun_stats[gun_stats_new_col_order]

    return gun_events, demography, gun_stats
