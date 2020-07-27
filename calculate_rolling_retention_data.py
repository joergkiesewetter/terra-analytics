import json
import os
from datetime import timedelta, datetime

import calculate_daily_transaction_data
import calculate_total_user_data
import config
from manage_transactions import get_first_transaction_timestamp
from util import logging

STORE_ROLLING_RETENTION_DATA = '/terra-data/raw/stats_rolling_retention_data'

log = logging.get_custom_logger(__name__, config.LOG_LEVEL)


def calculate_rolling_retention_data():

    os.makedirs(STORE_ROLLING_RETENTION_DATA, exist_ok=True)

    max_time = datetime.utcnow()
    max_time = max_time.replace(hour=0, minute=0, second=0, microsecond=0)

    stop_processing = False

    date_to_process = get_first_transaction_timestamp()

    log.debug('calculate: rolling retention')

    if date_to_process >= max_time:
        return

    total_user_from_yesterday = calculate_total_user_data.get_data_for_date(max_time - timedelta(days=1))

    while not stop_processing:

        log.debug('creating rolling retention data for ' + date_to_process.strftime('%Y-%m-%d'))

        final_data = _calculate_retention_data(date_to_process, total_user_from_yesterday)

        for currency in final_data.keys():
            file_path = os.path.join(STORE_ROLLING_RETENTION_DATA, currency, date_to_process.strftime('%Y-%m-%d') + '.json')

            os.makedirs(os.path.join(STORE_ROLLING_RETENTION_DATA, currency), exist_ok=True)

            with open(file_path, 'w') as file:
                file.write(json.dumps(final_data[currency]))

        date_to_process += timedelta(days=1)

        if date_to_process >= max_time:
            stop_processing = True


def _calculate_retention_data(start_date, total_user_from_yesterday):

    user_data_start_date = calculate_daily_transaction_data.get_user(start_date)

    retention_data = {}

    for currency in user_data_start_date:

        if currency not in retention_data.keys():
            retention_data[currency] = {}

        user_list_start_date = [value['address'] for value in user_data_start_date[currency]]

        timestamp_7d = (start_date + timedelta(days=7)).timestamp()
        timestamp_14d = (start_date + timedelta(days=14)).timestamp()
        timestamp_30d = (start_date + timedelta(days=30)).timestamp()

        user_list_7d = [key for (key, value) in total_user_from_yesterday[currency].items() if
                        int(value['last_seen_timestamp']) >= timestamp_7d]
        user_list_14d = [key for (key, value) in total_user_from_yesterday[currency].items() if
                         int(value['last_seen_timestamp']) >= timestamp_14d]
        user_list_30d = [key for (key, value) in total_user_from_yesterday[currency].items() if
                         int(value['last_seen_timestamp']) >= timestamp_30d]

        user_list_7d_intersection = _intersection(user_list_start_date, user_list_7d)
        user_list_14d_intersection = _intersection(user_list_start_date, user_list_14d)
        user_list_30d_intersection = _intersection(user_list_start_date, user_list_30d)

        retention_data[currency]['7d'] = len(user_list_7d_intersection) / len(user_list_start_date)
        retention_data[currency]['14d'] = len(user_list_14d_intersection) / len(user_list_start_date)
        retention_data[currency]['30d'] = len(user_list_30d_intersection) / len(user_list_start_date)

    return retention_data


def _intersection(lst1, lst2):
    return set(lst1).intersection(lst2)


def _get_last_processed_date():
    directories = [f for f in os.listdir(STORE_ROLLING_RETENTION_DATA) if
                   os.path.isdir(os.path.join(STORE_ROLLING_RETENTION_DATA, f))]

    last_file_timestamp = datetime.strptime('1970-01-01', '%Y-%m-%d')

    for directory in directories:

        target_dir = os.path.join(STORE_ROLLING_RETENTION_DATA, directory)

        files = [f for f in os.listdir(target_dir) if os.path.isfile(os.path.join(target_dir, f))]

        # get the file with the highest timestamp
        for file in files:

            line_parts = file.split('.')
            this_timestamp = datetime.strptime(line_parts[0], '%Y-%m-%d')
            last_file_timestamp = max(last_file_timestamp, this_timestamp)

    return last_file_timestamp


def get_retention_for_date(day, currency):

    day_string = day.strftime('%Y-%m-%d')
    file_path = os.path.join(STORE_ROLLING_RETENTION_DATA, currency, day_string + '.json')

    if not os.path.isfile(file_path):
        return {}

    with open(file_path, 'r') as file:
        content = json.load(file)

        return content
