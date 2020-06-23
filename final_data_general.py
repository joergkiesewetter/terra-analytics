import json
import os
from datetime import datetime, timedelta

import calculate_daily_payment_data
import calculate_market_data
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
    # date_last_processed = _get_last_processed_date()
    # date_to_process = max(date_to_process, date_last_processed + timedelta(days=1))

    log.debug('generate final data: general')

    if date_to_process >= max_time:
        return

    while not stop_processing:

        final_data = {}
        payment_data = calculate_daily_payment_data.get_data_for_date(date_to_process)

        file_path = os.path.join(STORE_FINAL_DATA_GENERAL, date_to_process.strftime('%Y-%m-%d') + '.json')

        if not os.path.isfile(file_path):
            for symbol in payment_data.keys():

                final_data[symbol] = {}

                log.debug('creating final general data for ' + date_to_process.strftime('%Y-%m-%d'))

                # Amount of Coins
                # Velocity

                market_data = calculate_market_data.get_data(symbol, date_to_process)

                final_data[symbol]['amount_of_coins'] = market_data['circulating_supply']
                final_data[symbol]['velocity_m1'] = payment_data[symbol]['total_amount'] / market_data['circulating_supply']

            if len(final_data.keys()) > 0:
                with open(file_path, 'w') as file:
                    file.write(json.dumps(final_data))

        date_to_process += timedelta(days=1)

        if date_to_process >= max_time:
            stop_processing = True
