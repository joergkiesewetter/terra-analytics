import json
import os
from datetime import timedelta, datetime

import pytz

import calculate_daily_transaction_data
import calculate_total_user_data
import config
from manage_transactions import get_first_transaction_timestamp
from util import logging

STORE_DAILY_RETENTION_DATA = '/terra-data/v2/raw/stats_daily_retention_data'

log = logging.get_custom_logger(__name__, config.LOG_LEVEL)


def calculate_daily_retention_data():

    os.makedirs(STORE_DAILY_RETENTION_DATA, exist_ok=True)

    max_time = datetime.utcnow()
    max_time = max_time.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.UTC)

    stop_processing = False

    date_to_process = get_first_transaction_timestamp()
    date_last_processed = _get_last_processed_date()
    date_to_process = max(date_to_process, date_last_processed - timedelta(days=31))

    log.debug('calculate: retention')

    if date_to_process >= max_time:
        return

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

    new_user_data = calculate_total_user_data.get_new_user_for_day(start_date)

    retention_data = {}

    date_7d = start_date + timedelta(days=7)
    date_14d = start_date + timedelta(days=14)
    date_30d = start_date + timedelta(days=30)

    user_data_7d = calculate_daily_transaction_data.get_user(date_7d)
    user_data_14d = calculate_daily_transaction_data.get_user(date_14d)
    user_data_30d = calculate_daily_transaction_data.get_user(date_30d)

    for currency in new_user_data:


        if currency not in retention_data.keys():
            retention_data[currency] = {}

        new_user_list = [key for (key, value) in new_user_data[currency].items()]

        if len(new_user_list) <= 0:
            retention_data[currency]['7d'] = 0
            retention_data[currency]['14d'] = 0
            retention_data[currency]['30d'] = 0
            continue

        if currency in user_data_7d:
            user_list_7d = [value['address'] for value in user_data_7d[currency]]
            user_list_7d_intersection = _intersection(new_user_list, user_list_7d)
            retention_data[currency]['7d'] = len(user_list_7d_intersection) / len(new_user_list)
        else:
            retention_data[currency]['7d'] = 0

        if currency in user_data_14d:
            user_list_14d = [value['address'] for value in user_data_14d[currency]]
            user_list_14d_intersection = _intersection(new_user_list, user_list_14d)
            retention_data[currency]['14d'] = len(user_list_14d_intersection) / len(new_user_list)
        else:
            retention_data[currency]['14d'] = 0

        if currency in user_data_30d:
            user_list_30d = [value['address'] for value in user_data_30d[currency]]
            user_list_30d_intersection = _intersection(new_user_list, user_list_30d)
            retention_data[currency]['30d'] = len(user_list_30d_intersection) / len(new_user_list)
        else:
            retention_data[currency]['30d'] = 0


    return retention_data


def _intersection(lst1, lst2):
    return set(lst1).intersection(lst2)


def _get_last_processed_date():
    directories = [f for f in os.listdir(STORE_DAILY_RETENTION_DATA) if
             os.path.isdir(os.path.join(STORE_DAILY_RETENTION_DATA, f))]

    last_file_timestamp = datetime.strptime('1970-01-01', '%Y-%m-%d')
    last_file_timestamp = last_file_timestamp.replace(tzinfo=pytz.UTC)

    for directory in directories:

        target_dir = os.path.join(STORE_DAILY_RETENTION_DATA, directory)

        files = [f for f in os.listdir(target_dir) if os.path.isfile(os.path.join(target_dir, f))]

        # get the file with the highest timestamp
        for file in files:

            if file.startswith('.'):
                continue

            line_parts = file.split('.')
            this_timestamp = datetime.strptime(line_parts[0], '%Y-%m-%d')
            this_timestamp = this_timestamp.replace(tzinfo=pytz.UTC)
            last_file_timestamp = max(last_file_timestamp, this_timestamp)

    return last_file_timestamp


def get_retention_for_date(day, currency):

    day_string = day.strftime('%Y-%m-%d')
    file_path = os.path.join(STORE_DAILY_RETENTION_DATA, currency, day_string + '.json')

    if not os.path.isfile(file_path):
        return {}

    with open(file_path, 'r') as file:
        content = json.load(file)

        return content
