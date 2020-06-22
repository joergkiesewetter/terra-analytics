import json
import os
from datetime import datetime, timedelta

import calculate_daily_payment_data
import config
from manage_transactions import get_first_transaction_timestamp
from util import logging

STORE_FINAL_DATA_PAYMENTS = '/terra-data/final/payments'

log = logging.get_custom_logger(__name__, config.LOG_LEVEL)


def final_data_payments():
    os.makedirs(STORE_FINAL_DATA_PAYMENTS, exist_ok=True)

    max_time = datetime.utcnow()
    max_time = max_time.replace(hour=0, minute=0, second=0, microsecond=0)

    stop_processing = False

    date_to_process = get_first_transaction_timestamp()
    date_last_processed = _get_last_processed_date()
    date_to_process = max(date_to_process, date_last_processed + timedelta(days=1))

    log.debug('generate final data: general')

    if date_to_process >= max_time:
        return

    while not stop_processing:

        log.debug('creating final payment data for ' + date_to_process.strftime('%Y-%m-%d'))

        # Amount
        # Count
        # Average

        final_data = calculate_daily_payment_data.get_data_for_date(date_to_process)

        if len(final_data.keys()) > 0:
            with open(os.path.join(STORE_FINAL_DATA_PAYMENTS, date_to_process.strftime('%Y-%m-%d') + '.json'), 'a') as file:

                for currency in final_data.keys():

                    if final_data[currency]['payment_count'] > 0:
                        final_data[currency]['average'] = final_data[currency]['total_amount'] / final_data[currency]['payment_count']
                    else:
                        final_data[currency]['average'] = 0

                file.write(json.dumps(final_data))

        date_to_process += timedelta(days=1)

        if date_to_process >= max_time:
            stop_processing = True


def _get_last_processed_date():
    files = [f for f in os.listdir(STORE_FINAL_DATA_PAYMENTS) if os.path.isfile(os.path.join(STORE_FINAL_DATA_PAYMENTS, f))]

    last_file_timestamp = datetime.strptime('1970-01-01', '%Y-%m-%d')

    # get the file with the highest timestamp
    for file in files:
        this_timestamp = datetime.strptime(file.split('.')[0], '%Y-%m-%d')

        last_file_timestamp = max(last_file_timestamp, this_timestamp)

    return last_file_timestamp
