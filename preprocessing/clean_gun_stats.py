from utils.data_funcs import to_datetime, select_attributes

ATTR_SELECTED = ['state', 'month', 'permit', 'totals', 'handgun', 'long_gun', 'other', 'multiple']
ATTR_TO_DATE = 'month'


def clean_gun_stats(raw_data):
    """
    Executes cleaning pipeline in gun-stats data
    :param raw_data: pd.DataFrame containing raw data to be cleaned
    :return: pd.DataFrame with cleaned data
    """
    data = select_attributes(raw_data, ATTR_SELECTED)
    data = to_datetime(data, ATTR_TO_DATE)
    data = delete_before_2013(data)
    return data


def delete_before_2013(data):
    """
    :param data: pd.DataFrame with data
    :return: pd.DataFrame with entries above year 2013
    """
    return data[data['month'] >= '01/01/2013']
