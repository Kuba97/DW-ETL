import re

import pandas as pd

from utils.data_funcs import select_attributes, to_datetime

ATTR_SELECTED = ['date', 'state', 'city_or_county', 'n_killed', 'n_injured', 'n_guns_involved', 'participant_age',
                 'participant_status', 'participant_type', 'participant_gender']


def clean_gun_events(data):
    """
    Executes cleaning pipeline in gun incidents data
    :param data: pd.DataFrame with gun incidents data
    :return: tuple of pd.DataFrames with gun incidents and gun participants data
    """
    data = select_attributes(data, ATTR_SELECTED)
    events, participants = split_into_two_data_frames(data)
    events = to_datetime(events, 'date')
    participants = parse_participant_rows(participants)
    participants = expand_particpant_status_to_binary(participants)
    participants = map_gender_to_single_char(participants)
    return events, participants


def split_into_two_data_frames(data):
    """
    Splitting data about gun incidents into DataFrames containing info separately for gun events and event participants
    :param data: pd.DataFrame
    :return: tuple of pd.DataFrames separately for events and participants
    """
    events = data[['date', 'state', 'city_or_county', 'n_killed', 'n_injured', 'n_guns_involved']]
    participants = data[['participant_age', 'participant_gender', 'participant_status', 'participant_type']]
    return events, participants


def parse_participant_rows(participants):
    """
    Parses data about participant from string part_0::attribute||part_1::attribute||...
    :param participants: pd.DataFrame
    :return: pd.DataFrame with data parsed and split into many rows
    """
    participants = _parse_attribute(participants['participant_age']).reset_index() \
        .merge(on=['level_0', 'level_1'], right=_parse_attribute(participants['participant_gender']).reset_index(),
               how='outer') \
        .merge(on=['level_0', 'level_1'], right=_parse_attribute(participants['participant_status']).reset_index(),
               how='outer') \
        .merge(on=['level_0', 'level_1'], right=_parse_attribute(participants['participant_type']).reset_index(),
               how='outer').set_index(['level_0', 'level_1'])
    participants.columns = ['age', 'gender', 'status', 'type']
    return participants


def expand_particpant_status_to_binary(participants):
    """
    Expands participants' status into binary columns (since participant may have many statuses)
    :param participants: pd.DataFrame
    :return: pd.DataFrame with exapnded columns
    """
    split_status = participants['status'].str.get_dummies(', ')
    participants = participants.drop(['status'], axis=1)
    participants = pd.concat([participants, split_status], axis=1)
    return participants


def map_gender_to_single_char(participants):
    """
    Maps gender full names ('Male', 'Female') to 'M' or 'F' char only
    :param participants: pd.DataFrame
    :return: pd.DataFrame
    """
    participants.loc[participants['gender'].notnull(), 'gender'] = participants.loc[
        participants['gender'].notnull(), 'gender'].map(lambda x: 'F' if x.lower().startswith('f') else 'M')
    return participants


def _parse_attribute(data):
    """
    Performs low level parsing at pd.Series from string like part_0::attribute||part_1::attribute||...
    to pd.Series (splitting into many rows)
    :param data: pd.Series
    :return: ps.Series parsed
    """
    r_split_index = re.compile(r"\:+")
    pre_split = data.str.split('|')
    split = pre_split[pre_split.notnull()].map(
        lambda x: {re.split(r_split_index, y)[0]: re.split(r_split_index, y)[1] for y in x if y != ''}).to_dict()
    index, series = zip(*[((i, j), split[i][j]) for i in split for j in split[i]])
    index = pd.MultiIndex.from_tuples(index)
    series = pd.Series(series, index=index)
    return pd.DataFrame(series, index=index)
