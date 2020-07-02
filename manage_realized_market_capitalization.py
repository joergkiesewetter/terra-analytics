import json
import os
from datetime import datetime, timedelta

import config
from manage_transactions import get_first_transaction_timestamp, get_transaction_data
from util import logging

STORE_REALIZED_MARKET_CAP_DATA = '/terra-data/raw/realized_market_cap'

log = logging.get_custom_logger(__name__, config.LOG_LEVEL)


def update_realized_market_capitalization():

    os.makedirs(STORE_REALIZED_MARKET_CAP_DATA, exist_ok=True)

    max_time = datetime.utcnow()
    max_time = max_time.replace(hour=0, minute=0, second=0, microsecond=0)

    stop_processing = False

    date_to_process = get_first_transaction_timestamp()
    date_last_processed = _get_last_processed_date()
    date_to_process = max(date_to_process, date_last_processed + timedelta(days=1))

    if date_to_process >= max_time:
        return

    state = _load_state(date_to_process)

    log.debug('calculate: realized market cap')

    while not stop_processing:

        transactions = get_transaction_data(date_to_process, type_filter=['bank_MsgMultiSend', 'bank_MsgSend'])

        log.debug('processing realized market cap for: ' + str(date_to_process))

        for transaction in transactions:

            start_date = datetime.now()

            type = transaction[0]
            block = transaction[1]
            timestamp = transaction[2]
            tx_hash = transaction[3]
            amount = int(transaction[4])
            currency = transaction[5]
            from_address = transaction[6]
            to_address = transaction[7]
            tax_amount = int(transaction[8])
            tax_currency = transaction[9]

            price = 0.1
            # TODO get the correct market price per token per date
            # first_market_price_date = get_first_market_price_date(symbol)
            #
            # if not first_market_price_date:
            #     log.debug("no market price available")
            #     return
            #
            # if int(timestamp) < first_market_price_date.timestamp():
            #
            #     if init_price:
            #         price = init_price
            #     else:
            #         price = 0
            # else:
            #     price = get_local_exchange_rate(symbol, datetime.utcfromtimestamp(int(timestamp)))

            if currency not in state.keys():
                state[currency] = {}

            if from_address in state[currency].keys():
                from_account = state[currency][from_address]
            else:
                from_account = None

            if to_address in state[currency].keys():
                to_account = state[currency][to_address]
            else:
                to_account = {
                    'balance': 0,
                    'data': [],
                }
                state[currency][to_address] = to_account

            #
            # add transaction to the from-account
            #

            if from_account:

                remaining_value = amount

                while remaining_value > 0:
                    try:
                        from_amount = from_account['data'][0][1]
                    except Exception:
                        # log.debug(transaction)
                        break

                    if remaining_value < from_amount:
                        from_account['data'][0][1] -= remaining_value
                        remaining_value = 0
                        from_account['data'][0][2] = price

                    else:
                        remaining_value -= from_amount
                        from_account['data'] = from_account['data'][1:]



                from_account['balance'] = max(0, int(from_account['balance']) - amount)


            #
            # add transaction to the to-account
            #

            to_account['data'].append([timestamp, amount, price])
            to_account['balance'] = int(to_account['balance']) + amount

            end_date = datetime.now()
            # print('calculation time: ' + str((end_date - start_date).total_seconds() * 1000))

        #
        # all transactions are processed, saving state to a file
        #
        _save_state(date_to_process, state)

        date_to_process = date_to_process + timedelta(days=1)

        if date_to_process >= max_time:
            stop_processing = True


def _get_last_processed_date():
    directories = [f for f in os.listdir(STORE_REALIZED_MARKET_CAP_DATA) if
                   os.path.isdir(os.path.join(STORE_REALIZED_MARKET_CAP_DATA, f))]

    last_file_timestamp = datetime.strptime('1970-01-01', '%Y-%m-%d')

    for directory in directories:

        target_dir = os.path.join(STORE_REALIZED_MARKET_CAP_DATA, directory)

        files = [f for f in os.listdir(target_dir) if os.path.isfile(os.path.join(target_dir, f))]

        # get the file with the highest timestamp
        for file in files:
            line_parts = file.split('.')
            this_timestamp = datetime.strptime(line_parts[0], '%Y-%m-%d')
            last_file_timestamp = max(last_file_timestamp, this_timestamp)

    return last_file_timestamp


def _load_state(date_to_process):

    date_to_load = date_to_process - timedelta(days=1)

    currencies = [f for f in os.listdir(STORE_REALIZED_MARKET_CAP_DATA) if
                   os.path.isdir(os.path.join(STORE_REALIZED_MARKET_CAP_DATA, f))]

    return_data = {}

    for currency in currencies:
        path = os.path.join(STORE_REALIZED_MARKET_CAP_DATA, currency, date_to_load.strftime('%Y-%m-%d') + '.csv')

        if not os.path.isfile(path):
            continue

        return_data[currency] = {}

        with open(path, 'rt') as file:

            for line in file:

                line_parts = line.split(';')

                return_data[currency][line_parts[0]] = {
                    'balance': line_parts[1],
                    'data': json.loads(line_parts[2])
                }

    return return_data


def _save_state(date_to_process, state):

    for currency in state.keys():

        path = os.path.join(STORE_REALIZED_MARKET_CAP_DATA, currency)

        os.makedirs(path, exist_ok=True)

        path = os.path.join(path, date_to_process.strftime('%Y-%m-%d') + '.csv')

        with open(path, 'w') as file:
            for key, value in state[currency].items():
                if len(value['data']) > 0:
                    file.write(key + ';' + str(value['balance']) + ';' + json.dumps(value['data']) + '\n')


def get_first_data_timestamp(token):

    last_file_timestamp = None

    root_directory = os.path.join(STORE_REALIZED_MARKET_CAP_DATA, token)

    files = [f for f in os.listdir(root_directory) if os.path.isfile(os.path.join(root_directory, f))]

    # get the file with the highest timestamp
    for file in files:
        filename = file.split('.')[0]

        timestamp = datetime.strptime(filename, '%Y-%m-%d')

        if not last_file_timestamp or timestamp < last_file_timestamp:
            last_file_timestamp = timestamp

    return last_file_timestamp


def get_data(date, token):
    try:
        with open(os.path.join(STORE_REALIZED_MARKET_CAP_DATA, token, date.strftime('%Y-%m-%d') + '.csv'), 'rt') as file:

            return_data = []

            for line in file:
                return_data.append(line.split(';'))

            for datum in return_data:
                datum[2] = json.loads(datum[2])

            return return_data
    except:
        return []


def get_token_list():
    return [f for f in os.listdir(STORE_REALIZED_MARKET_CAP_DATA) if os.path.isdir(os.path.join(STORE_REALIZED_MARKET_CAP_DATA, f))]
