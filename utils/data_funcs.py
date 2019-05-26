import numpy as np
import pandas as pd


def select_attributes(data, attributes):
    """
    Selects attributes according to specified list
    :param attributes: list of attributes to be selected
    :param data: pd.DataFrame with data
    :return: pd.DataFrame with selected attributes only
    """
    return data[attributes]


def to_datetime(data, column):
    """
    Casting str column to datetime with format compatible with SqlServer
    :param data: pd.DataFrame with data
    :param column: list(str) of columns to be casted to datetime format
    :return: pd.DataFrame with columns casted to datetime
    """
    data.loc[:, column] = pd.to_datetime(data[column]).dt.strftime("%d/%m/%Y")
    return data


def cast_to_int(data, columns):
    """
    :param data: pd.DataFrame containing columns to be casted
    :param columns: list(str) with columns to be casted
    :return: pd.DataFrame with columns casted to int
    """
    data[columns] = data[columns].fillna(-1).astype(int).astype(str).replace('-1', np.nan)
    return data


def rename_columns(data, col_name_mapping):
    """
    :param data: pd.DataFrame with data containing columns to be renamed
    :param col_name_mapping: dict{old_name: new_name}
    :return: pd.DataFrame with renamed columns
    """
    return data.rename(columns=col_name_mapping)
