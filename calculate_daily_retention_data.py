import json
import os
from datetime import timedelta, datetime

import calculate_daily_transaction_data
import config
from manage_transactions import get_first_transaction_timestamp
from util import logging

STORE_DAILY_RETENTION_DATA = '/terra-data/v2/raw/stats_daily_retention_data'

log = logging.get_custom_logger(__name__, config.LOG_LEVEL)


def calculate_daily_retention_data():

    os.makedirs(STORE_DAILY_RETENTION_DATA, exist_ok=True)

    max_time = datetime.utcnow()
    max_time = max_time.replace(hour=0, minute=0, second=0, microsecond=0)

    stop_processing = False

    date_to_process = get_first_transaction_timestamp()
    date_last_processed = _get_last_processed_date()
    date_to_process = max(date_to_process, date_last_processed + timedelta(days=1))

    log.debug('calculate: retention')

    if date_to_process >= max_time:
        return

    # with open(symbol_file, 'a') as file:
    while not stop_processing:

        log.debug('creating retention data for ' + date_to_process.strftime('%Y-%m-%d'))

        final_data = _calculate_retention_data(date_to_process)

        for currency in final_data.keys():
            file_path = os.path.join(STORE_DAILY_RETENTION_DATA, currency, date_to_process.strftime('%Y-%m-%d') + '.json')

            os.makedirs(os.path.join(STORE_DAILY_RETENTION_DATA, currency), exist_ok=True)

            with open(file_path, 'w') as file:
                file.write(json.dumps(final_data[currency]))

        date_to_process += timedelta(days=1)

        if date_to_process >= max_time:
            stop_processing = True


def _calculate_retention_data(start_date):

    user_data_start_date = calculate_daily_transaction_data.get_user(start_date)

    retention_data = {}

    for currency in user_data_start_date:

        if currency not in retention_data.keys():
            retention_data[currency] = {}

        user_list_start_date = [value['address'] for value in user_data_start_date[currency]]

        for i in range(1, 30):

            date1 = start_date - timedelta(days=i)
            date1_string = date1.strftime('%Y-%m-%d')
            user_data_date1 = calculate_daily_transaction_data.get_user(date1)

            if currency not in user_data_date1.keys():
                continue

            user_list_date1 = [value['address'] for value in user_data_date1[currency]]
            user_list_intersection = _intersection(user_list_start_date, user_list_date1)

            if date1_string not in retention_data[currency].keys():
                retention_data[currency][date1_string] = len(user_list_intersection) / len(user_list_date1)

    return retention_data


def _intersection(lst1, lst2):
    return set(lst1).intersection(lst2)


def _get_last_processed_date():
    directories = [f for f in os.listdir(STORE_DAILY_RETENTION_DATA) if
             os.path.isdir(os.path.join(STORE_DAILY_RETENTION_DATA, f))]

    last_file_timestamp = datetime.strptime('1970-01-01', '%Y-%m-%d')

    for directory in directories:

        target_dir = os.path.join(STORE_DAILY_RETENTION_DATA, directory)

        files = [f for f in os.listdir(target_dir) if os.path.isfile(os.path.join(target_dir, f))]

        # get the file with the highest timestamp
        for file in files:

            line_parts = file.split('.')
            this_timestamp = datetime.strptime(line_parts[0], '%Y-%m-%d')
            last_file_timestamp = max(last_file_timestamp, this_timestamp)

    return last_file_timestamp


def get_retention_for_date(last_day, currency):

    firt_day = last_day - timedelta(days=30)

    return_data = {}

    for i in reversed(range(1, 31)):

        date1 = last_day - timedelta(days=i)
        date1_string = date1.strftime('%Y-%m-%d')

        return_data[date1_string] = []

        for j in range(0, i):
            date2 = last_day - timedelta(days=j)
            date2_string = date2.strftime('%Y-%m-%d')

            file_path = os.path.join(STORE_DAILY_RETENTION_DATA, currency, date2_string + '.json')

            if not os.path.isfile(file_path):
                continue

            with open(file_path, 'r') as file:
                content = json.load(file)

                if date1_string not in content.keys():
                    continue

                value = content[date1_string]
                return_data[date1_string].append({
                    'day': i-j,
                    'value': value,
                })

    return return_data
