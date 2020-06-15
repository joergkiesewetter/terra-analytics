import os
from datetime import datetime, timedelta

import config
from manage_transactions import get_first_transaction_timestamp
from util import logging

STORE_FINAL_DATA_GENERAL = '/terra-data/final/general'

log = logging.get_custom_logger(__name__, config.LOG_LEVEL)


def final_data_general():

    os.makedirs(STORE_FINAL_DATA_GENERAL, exist_ok=True)

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

        log.debug('creating final data for ' + date_to_process.strftime('%Y-%m-%d'))

        # Amount of Coins
        # Velocity
        # retention 7d
        # retention 14d
        # retention 30d

        date_to_process += timedelta(days=1)

        if date_to_process >= max_time:
            stop_processing = True
