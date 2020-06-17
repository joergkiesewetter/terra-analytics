import json
import os
from datetime import datetime, timedelta

import calculate_daily_transaction_data
import config
from manage_transactions import get_first_transaction_timestamp
from util import logging

STORE_FINAL_DATA_USER = '/terra-data/final/user'

log = logging.get_custom_logger(__name__, config.LOG_LEVEL)


def final_data_user():
    os.makedirs(STORE_FINAL_DATA_USER, exist_ok=True)

    max_time = datetime.utcnow()
    max_time = max_time.replace(hour=0, minute=0, second=0, microsecond=0)

    stop_processing = False

    date_to_process = get_first_transaction_timestamp()
    # date_last_processed = _get_last_processed_date()
    # date_to_process = max(date_to_process, date_last_processed + timedelta(days=1))

    log.debug('generate final data: user')

    if date_to_process >= max_time:
        return

    user_list = {}

    while not stop_processing:

        user_meta_data = {}

        log.debug('creating final user data for ' + date_to_process.strftime('%Y-%m-%d'))

        # Total
        # New
        # Daily Active

        file_path = os.path.join(STORE_FINAL_DATA_USER, date_to_process.strftime('%Y-%m-%d') + '.json')
        raw_data = calculate_daily_transaction_data.get_user(date_to_process)

        for currency in raw_data.keys():

            if currency not in user_list.keys():
                user_list[currency] = set()

            user_meta_data[currency] = {}

            count_new_user = 0
            count_daily_active = len(raw_data[currency])

            for user_object in raw_data[currency]:

                if not user_object['address'] in user_list[currency]:
                    count_new_user += 1

                user_list[currency].add(user_object['address'])

            user_meta_data[currency]['new'] = count_new_user
            user_meta_data[currency]['daily'] = count_daily_active

        if not os.path.isfile(file_path):

            #
            # calculate retention data
            #
            retention_data = _calculate_retention_data(date_to_process)

            final_data = {}

            for currency in user_list.keys():
                final_data[currency] = {
                    'total': len(user_list[currency]),
                    'new': user_meta_data.get(currency)['new'] if (currency in user_meta_data) else 0,
                    'daily': user_meta_data.get(currency)['daily'] if (currency in user_meta_data) else 0,
                    'retention': retention_data.get(currency),
                }

            if len(raw_data.keys()) > 0:
                with open(file_path, 'w') as file:
                    file.write(json.dumps(final_data))

        date_to_process += timedelta(days=1)

        if date_to_process >= max_time:
            stop_processing = True


def _calculate_retention_data(start_date):

    user_data_start_date = calculate_daily_transaction_data.get_user(start_date)

    retention_data = {}

    for currency in user_data_start_date:

        if currency not in retention_data.keys():
            retention_data[currency] = {}

        for i in range(0, 29):

            date1 = start_date - timedelta(days=i)
            date1_string = date1.strftime('%Y-%m-%d')
            user_data_date1 = calculate_daily_transaction_data.get_user(date1)

            if currency not in user_data_date1.keys():
                continue

            user_list_date1 = [value['address'] for value in user_data_date1[currency]]

            for j in range(i+1, 30):

                date2 = start_date - timedelta(days=j)
                date2_string = date2.strftime('%Y-%m-%d')
                user_data_date2 = calculate_daily_transaction_data.get_user(date2)

                if currency not in user_data_date2.keys():
                    continue

                user_list_date2 = [value['address'] for value in user_data_date2[currency]]
                user_list_intersection = [value for value in user_list_date1 if value in user_list_date2]

                if date2_string not in retention_data[currency].keys():
                    retention_data[currency][date2_string] = []

                retention_data[currency][date2_string].append({
                    'day': (date1 - date2).days,
                    'value': len(user_list_intersection) / len(user_list_date2)
                })

    return retention_data




