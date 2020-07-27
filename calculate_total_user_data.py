import os
import traceback
from datetime import datetime, timedelta

import config
from manage_transactions import get_first_transaction_timestamp, get_transaction_data
from util import logging

# structure /terra-data/raw/stats_daily_address_payments/<token>/<date>.csv
STORE_TOTAL_USER_DIRECTORY = '/terra-data/raw/stats_total_user_data'

log = logging.get_custom_logger(__name__, config.LOG_LEVEL)


def calculate_total_user_data():
    os.makedirs(STORE_TOTAL_USER_DIRECTORY, exist_ok=True)

    max_time = datetime.utcnow()
    max_time = max_time.replace(hour=0, minute=0, second=0, microsecond=0)

    stop_processing = False

    date_to_process = get_first_transaction_timestamp()
    date_last_processed = _get_last_processed_date()
    date_to_process = max(date_to_process, date_last_processed + timedelta(days=1))

    log.debug('calculate: total user data')

    if date_to_process >= max_time:
        return

    state = _load_state(date_to_process)

    while not stop_processing:

        log.debug('analysing total user data for ' + date_to_process.strftime('%Y-%m-%d'))

        transactions = get_transaction_data(date_to_process, type_filter=['bank_MsgMultiSend', 'bank_MsgSend'])

        for transaction in transactions:

            # log.debug(transaction)

            type = transaction[0]
            block = transaction[1]
            timestamp = transaction[2]
            tx_hash = transaction[3]
            amount = int(transaction[4])
            currency = transaction[5]
            from_address = transaction[6]
            to_address = transaction[7]
            tax_amount = transaction[8]
            tax_currency = transaction[9]

            if currency not in state.keys():
                state[currency] = {}

            if from_address not in state[currency]:
                state[currency][from_address] = {
                    'first_seen_timestamp': timestamp,
                }

            state[currency][from_address]['last_seen_timestamp'] = timestamp


        _save_state(date_to_process, state)

        date_to_process += timedelta(days=1)

        if date_to_process >= max_time:
            stop_processing = True


def _load_state(date_to_process):

    date_to_load = date_to_process - timedelta(days=1)

    currencies = [f for f in os.listdir(STORE_TOTAL_USER_DIRECTORY) if
                   os.path.isdir(os.path.join(STORE_TOTAL_USER_DIRECTORY, f))]

    return_data = {}

    for currency in currencies:
        path = os.path.join(STORE_TOTAL_USER_DIRECTORY, currency, date_to_load.strftime('%Y-%m-%d') + '.csv')

        if not os.path.isfile(path):
            continue

        return_data[currency] = {}

        with open(path, 'rt') as file:

            for line in file:

                line_parts = line.split(';')

                return_data[currency][line_parts[0]] = {
                    'first_seen_timestamp': line_parts[1],
                    'last_seen_timestamp': line_parts[2],
                }

    return return_data


def _save_state(date_to_process, state):

    for currency in state.keys():

        path = os.path.join(STORE_TOTAL_USER_DIRECTORY, currency)

        os.makedirs(path, exist_ok=True)

        path = os.path.join(path, date_to_process.strftime('%Y-%m-%d') + '.csv')

        with open(path, 'w') as file:
            for account, value in state[currency].items():
                file.write(account + ';' +
                           str(value['first_seen_timestamp']) + ';' +
                           str(value['last_seen_timestamp']) + '\n')


def _get_last_processed_date():
    directories = [f for f in os.listdir(STORE_TOTAL_USER_DIRECTORY) if
                   os.path.isdir(os.path.join(STORE_TOTAL_USER_DIRECTORY, f))]

    last_file_timestamp = datetime.strptime('1970-01-01', '%Y-%m-%d')

    for directory in directories:

        target_dir = os.path.join(STORE_TOTAL_USER_DIRECTORY, directory)

        files = [f for f in os.listdir(target_dir) if os.path.isfile(os.path.join(target_dir, f))]

        # get the file with the highest timestamp
        for file in files:
            line_parts = file.split('.')
            this_timestamp = datetime.strptime(line_parts[0], '%Y-%m-%d')
            last_file_timestamp = max(last_file_timestamp, this_timestamp)

    return last_file_timestamp


def get_data_for_date(date):
    directories = [f for f in os.listdir(STORE_TOTAL_USER_DIRECTORY) if
                   os.path.isdir(os.path.join(STORE_TOTAL_USER_DIRECTORY, f))]

    return_data = {}

    for token in directories:
        try:

            return_data[token] = {}

            with open(os.path.join(STORE_TOTAL_USER_DIRECTORY, token, date.strftime('%Y-%m-%d') + '.csv'), 'rt') as file:

                for line in file:

                    if len(line) <= 0:
                        continue

                    line_parts = line.split(';')

                    return_data[token][line_parts[0]] = {
                        'first_seen_timestamp': line_parts[1],
                        'last_seen_timestamp': line_parts[2],
                    }

        except:
            log.debug('error fetching data')
            traceback.print_exc()

    return return_data
