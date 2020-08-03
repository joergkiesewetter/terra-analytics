import os
import traceback
from datetime import datetime, timedelta

import pytz

import config
from manage_transactions import get_first_transaction_timestamp, get_transaction_data
from util import logging

# structure /terra-data/raw/stats_daily_payments/<token>.csv
STORE_DAILY_PAYMENTS_DIRECTORY = '/terra-data/v2/raw/stats_daily_payments'

# structure /terra-data/raw/stats_daily_address_payments/<token>/<date>.csv
STORE_DAILY_ADDRESS_PAYMENTS_DIRECTORY = '/terra-data/v2/raw/stats_daily_address_payments'

log = logging.get_custom_logger(__name__, config.LOG_LEVEL)


def calculate_daily_payment_data():

    os.makedirs(STORE_DAILY_PAYMENTS_DIRECTORY, exist_ok=True)

    max_time = datetime.utcnow()
    max_time = max_time.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.UTC)

    stop_processing = False

    date_to_process = get_first_transaction_timestamp()
    date_last_processed = _get_last_processed_date()
    date_to_process = max(date_to_process, date_last_processed + timedelta(days=1))

    log.debug('calculate: total amount of coins per coin')

    # TODO remove all lines from STORE_DAILY_PAYMENTS_DIRECTORY which are in the future from date_to_process
    # TODO remove all files from STORE_DAILY_ADDRESS_PAYMENTS_DIRECTORY which are in the future from date_to_process

    if date_to_process >= max_time:
        return

    # with open(symbol_file, 'a') as file:
    while not stop_processing:

        log.debug('analysing payment data for ' + date_to_process.strftime('%Y-%m-%d'))

        transactions = get_transaction_data(date_to_process, type_filter=['bank_MsgMultiSend', 'bank_MsgSend'])

        token = dict()

        for transaction in transactions:

            type = transaction[0]
            block = transaction[1]
            timestamp = transaction[2]
            tx_hash = transaction[3]
            currency = transaction[5]
            tax_amount = transaction[8]
            tax_currency = transaction[9]

            if currency not in token.keys():
                token[currency] = {
                    'total_amount': 0,
                    'payment_count': 0,
                    'total_tax_amount': 0,
                    'active_users': dict(),
                }

            amount = int(transaction[4])
            from_address = transaction[6]
            to_address = transaction[7]
            tax_amount = int(transaction[8])
            tax_currency = transaction[9]

            token[currency]['payment_count'] += 1
            token[currency]['total_amount'] += amount
            token[currency]['total_tax_amount'] += tax_amount

            if from_address not in token[currency]['active_users'].keys():
                token[currency]['active_users'][from_address] = {
                    'total_amount': 0,
                    'payment_count': 0,
                }

            token[currency]['active_users'][from_address]['total_amount'] += amount
            token[currency]['active_users'][from_address]['payment_count'] += 1

        for currency in token.keys():

            with open(os.path.join(STORE_DAILY_PAYMENTS_DIRECTORY, currency + '.csv'), 'a') as file:

                tax_rate = token[currency]['total_tax_amount'] / token[currency]['total_amount']

                file.write(','.join([date_to_process.strftime('%Y-%m-%d'),
                                     str(token[currency]['total_amount']),
                                     str(token[currency]['payment_count']),
                                     f"{tax_rate:.15f}",
                                    ]) + '\n')

            os.makedirs(os.path.join(STORE_DAILY_ADDRESS_PAYMENTS_DIRECTORY, currency), exist_ok=True)

            with open(os.path.join(STORE_DAILY_ADDRESS_PAYMENTS_DIRECTORY,
                                   currency,
                                   date_to_process.strftime('%Y-%m-%d') + '.csv'), 'a') as file:

                for address in token[currency]['active_users'].keys():
                    file.write(','.join([address,
                                         str(token[currency]['active_users'][address]['total_amount']),
                                         str(token[currency]['active_users'][address]['payment_count']),
                                        ]) + '\n')

        date_to_process += timedelta(days=1)

        if date_to_process >= max_time:
            stop_processing = True


def _get_last_processed_date():
    files = [f for f in os.listdir(STORE_DAILY_PAYMENTS_DIRECTORY) if os.path.isfile(os.path.join(STORE_DAILY_PAYMENTS_DIRECTORY, f))]

    last_file_timestamp = datetime.strptime('1970-01-01', '%Y-%m-%d')
    last_file_timestamp = last_file_timestamp.replace(tzinfo=pytz.UTC)

    # get the file with the highest timestamp
    for file in files:
        symbol_file = os.path.join(STORE_DAILY_PAYMENTS_DIRECTORY, file)

        with open(symbol_file, 'r') as file:

            for line in file:
                line_parts = line.split(',')

                this_timestamp = datetime.strptime(line_parts[0], '%Y-%m-%d')
                this_timestamp = this_timestamp.replace(tzinfo=pytz.UTC)

                last_file_timestamp = max(last_file_timestamp, this_timestamp)

    return last_file_timestamp


def get_data_for_date(date):
    files = [f for f in os.listdir(STORE_DAILY_PAYMENTS_DIRECTORY) if
             os.path.isfile(os.path.join(STORE_DAILY_PAYMENTS_DIRECTORY, f))]

    return_data = {}

    for filename in files:
        try:

            token_name = filename.split('.')[0]

            with open(os.path.join(STORE_DAILY_PAYMENTS_DIRECTORY, filename)) as file:

                for line in file:

                    if len(line) <= 0:
                        continue

                    line_parts = line.split(',')
                    line_date = datetime.strptime(line_parts[0], '%Y-%m-%d')

                    if line_date == date:
                        return_data[token_name] = {
                            'total_amount': int(line_parts[1]),
                            'payment_count': int(line_parts[2]),
                        }
                        break

        except:
            log.debug('error fetching data')
            traceback.print_exc()

    return return_data
