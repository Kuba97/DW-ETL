import pandas as pd


def load_data(path):
    """
    Loads csv file to DataFrame object
    :param path: str path to csv file
    :return: pd.DataFrame containing loaded data from csv
    """
    return pd.read_csv(path)


def store_data(df, path, with_index=True):
    """
    Saves DataFrame object into csv file
    :param df: pd.DataFrame with data to be exported to csv file
    :param path: str path to target file ends with .csv extension
    :param with_index: bool determining if data should be saved with auto-incremented index
    :return: None
    """
    df.to_csv(path, index=with_index)
