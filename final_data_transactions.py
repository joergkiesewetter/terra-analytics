import json
import os
from datetime import datetime, timedelta

import calculate_daily_transaction_data
import config
from manage_transactions import get_first_transaction_timestamp
from util import logging

STORE_FINAL_DATA_TRANSACTIONS = '/terra-data/final/transactions'

log = logging.get_custom_logger(__name__, config.LOG_LEVEL)


def final_data_transactions():
    os.makedirs(STORE_FINAL_DATA_TRANSACTIONS, exist_ok=True)

    max_time = datetime.utcnow()
    max_time = max_time.replace(hour=0, minute=0, second=0, microsecond=0)

    stop_processing = False

    date_to_process = get_first_transaction_timestamp()
    date_last_processed = _get_last_processed_date()
    date_to_process = max(date_to_process, date_last_processed + timedelta(days=1))

    log.debug('generate final data: transactions')

    if date_to_process >= max_time:
        return

    while not stop_processing:

        log.debug('creating final data for ' + date_to_process.strftime('%Y-%m-%d'))

        # Send
        # Deposit
        # Swap
        # Create Validator

        raw_data_send = calculate_daily_transaction_data.get_data(date_to_process, 'bank_MsgSend')
        raw_data_multisend = calculate_daily_transaction_data.get_data(date_to_process, 'bank_MsgMultiSend')
        raw_data_deposit = calculate_daily_transaction_data.get_data(date_to_process, 'gov_MsgDeposit')
        raw_data_swap = calculate_daily_transaction_data.get_data(date_to_process, 'market_MsgSwap')
        raw_data_create_validator = calculate_daily_transaction_data.get_data(date_to_process, 'staking_MsgCreateValidator')

        final_data = {}

        for raw_data in [raw_data_send,
                         raw_data_multisend,
                         raw_data_deposit,
                         raw_data_swap,
                         raw_data_create_validator]:
            for currency in raw_data.keys():
                if currency not in final_data.keys():
                    final_data[currency] = {}

                if 'send' not in final_data[currency]:
                    final_data[currency]['send'] = 0

        for currency in raw_data_send.keys():

            final_data[currency]['send'] += raw_data_send[currency]['count']

        for currency in raw_data_multisend.keys():
            final_data[currency]['send'] += raw_data_multisend[currency]['count']

        for currency in raw_data_deposit.keys():
            final_data[currency]['deposit'] = raw_data_deposit[currency]['count']

        for currency in raw_data_swap.keys():
            final_data[currency]['swap'] = raw_data_swap[currency]['count']

        for currency in raw_data_create_validator.keys():
            final_data[currency]['create_validator'] = raw_data_create_validator[currency]['count']

        if len(final_data.keys()) > 0:
            with open(os.path.join(STORE_FINAL_DATA_TRANSACTIONS, date_to_process.strftime('%Y-%m-%d') + '.json'), 'a') as file:
                file.write(json.dumps(final_data))

        date_to_process += timedelta(days=1)

        if date_to_process >= max_time:
            stop_processing = True


def _get_last_processed_date():
    files = [f for f in os.listdir(STORE_FINAL_DATA_TRANSACTIONS) if os.path.isfile(os.path.join(STORE_FINAL_DATA_TRANSACTIONS, f))]

    last_file_timestamp = datetime.strptime('1970-01-01', '%Y-%m-%d')

    # get the file with the highest timestamp
    for file in files:
        this_timestamp = datetime.strptime(file.split('.')[0], '%Y-%m-%d')

        last_file_timestamp = max(last_file_timestamp, this_timestamp)

    return last_file_timestamp
