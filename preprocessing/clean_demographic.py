import pandas as pd
from utils.data_funcs import select_attributes, rename_columns

ATTR_SELECTED = ['Year', 'State', 'County', 'TotalPop', 'Men', 'Women', 'Hispanic', 'White', 'Black', 'Native', 'Asian',
                 'Pacific', 'Citizen', 'Income', 'Poverty', 'ChildPoverty', 'Employed', 'Unemployment']
ATTR_NAME_MAPPING = {"County": "Country"}


def clean_demographic(raw_data_2015, raw_data_2017):
    """
    Executes cleaning pipeline in demographic data
    :param raw_data_2015: pd.DataFrame with data from year 2015
    :param raw_data_2017: pd.DataFrame with data from year 2017
    :return: pd.DataFrame with cleaned data
    """
    data = merge_add_year_column(raw_data_2015, raw_data_2017)
    data = select_attributes(data, ATTR_SELECTED)
    data = rename_columns(data, ATTR_NAME_MAPPING)
    data = delete_data(data)
    return data


def merge_add_year_column(data_2015, data_2017):
    """
    Merging two data from two year into one DataFrame with column about year added
    :param data_2015:
    :param data_2017:
    :return:
    """
    data_2015['Year'] = 2015
    data_2017['Year'] = 2017
    return pd.concat([data_2015, data_2017], axis=0, sort=False)


def delete_data(data):
    """
    Deletes data about state 'Puerto Rico' since it's not present in remaining data sets
    :param data: pd.DataFrame
    :return: pd.DataFrame without unnecessary rows
    """
    return data[data['State'] != 'Puerto Rico']
