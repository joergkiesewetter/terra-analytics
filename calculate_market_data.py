import json
import os
from datetime import date, datetime, timedelta

import config
import manage_realized_market_capitalization
from util import logging

STORE_MARKET_DATA = '/terra-data/raw/market_data/'

log = logging.get_custom_logger(__name__, config.LOG_LEVEL)


def calculate_market_data():

    os.makedirs(STORE_MARKET_DATA, exist_ok=True)

    currencies = manage_realized_market_capitalization.get_token_list()

    for symbol in currencies:

        symbol_file = os.path.join(STORE_MARKET_DATA, symbol + '.csv')

        max_time = datetime.utcnow()
        max_time = max_time.replace(hour=0, minute=0, second=0, microsecond=0)

        stop_processing = False

        date_to_process = manage_realized_market_capitalization.get_first_data_timestamp(symbol)

        if not date_to_process:
            log.debug('no date to calculate market data')
            return

        date_last_processed = _get_last_processed_date(symbol)
        date_to_process = max(date_to_process, date_last_processed + timedelta(days=1))

        log.debug('calculate realized market cap for ' + symbol)

        if date_to_process >= max_time:
            return

        with open(symbol_file, 'a') as file:
            while not stop_processing:

                data = manage_realized_market_capitalization.get_data(date_to_process, symbol)

                if len(data) <= 0:
                    return

                result = _analyse_data(symbol, data, date_to_process)

                date_string = date_to_process.strftime('%Y-%m-%d')
                result_string = date_string + ',' + \
                                str(result['circulating_supply']) + ',' + \
                                str(result['not_moved_coins']) + ',' + \
                                str(result['market_cap']) + ',' + \
                                str(result['realized_market_cap']) + ',' + \
                                str(result['mvrv']) + ',' + \
                                str(result['coins_older_1y']) + ',' + \
                                str(result['num_holder']) + ',' + \
                                str(result['exchange_rate'])
                file.write(result_string + '\n')
                file.flush()

                log.debug('calculate realized market cap for ' + symbol + ': ' + date_string)

                date_to_process += timedelta(days=1)

                if date_to_process >= max_time:
                    stop_processing = True


def _get_last_processed_date(symbol):
    symbol_file = os.path.join(STORE_MARKET_DATA, symbol + '.csv')

    last_file_timestamp = '1970-01-01'

    if not os.path.exists(symbol_file):
        return datetime.utcfromtimestamp(0)

    with open(symbol_file, 'r') as file:

        for line in file:
            line_parts = line.split(',')

            last_file_timestamp = line_parts[0]

    return datetime.strptime(last_file_timestamp, '%Y-%m-%d')


##
# return [[<holder_address>, <balance>, <token data>], ...]
#
# def _get_data_to_process(symbol, date):
#     try:
#         with open(os.path.join(BASE_DIRECTORY, symbol, date.strftime('%Y-%m-%d') + '.csv'), 'rt') as file:
#
#             return_data = []
#
#             for line in file:
#                 return_data.append(line.split(';'))
#
#             return return_data
#     except:
#         return []


def _analyse_data(symbol, data, date_to_process):

    return_data = {
        'circulating_supply': 0,
        'not_moved_coins': 0,
        'market_cap': 0,
        'realized_market_cap': 0,
        'mvrv': 0,
        'coins_older_1y': 0,
        'num_holder': 0,
        'exchange_rate': 0,
    }

    market_entry_date = datetime.strptime('2019-01-01', '%Y-%m-%d')
    # TODO fix it
    # market_entry_date = get_first_market_price_date(symbol)

    date_1y = _add_years(date_to_process, -1)

    exchange_rate = 0.1
    # TODO fix it
    # exchange_rate = util.get_local_exchange_rate(symbol, date_to_process)
    # if not exchange_rate:
    #     exchange_rate = token['init_price']

    for line in data:

        for coin_data in line[2]:

            # calculate number of coins never traded after market entry
            if int(coin_data[0]) < market_entry_date.timestamp():
                return_data['not_moved_coins'] += coin_data[1]

            # calculate total realized market cap
            amount_coins = coin_data[1]

            return_data['circulating_supply'] += amount_coins

            return_data['realized_market_cap'] += amount_coins * coin_data[2] * pow(10, 9)

            # calculate number of coins not moved for more than a year
            if int(coin_data[0]) < date_1y.timestamp():
                return_data['coins_older_1y'] += coin_data[1]


        # calculate num holder
        if (int(line[1]) / 1e9) * exchange_rate > 0.01:
        # if coin_data[1] > 1e20:
            return_data['num_holder'] += 1

    return_data['not_moved_coins'] /= pow(10, 9)
    return_data['coins_older_1y'] /= pow(10, 9)
    return_data['exchange_rate'] = exchange_rate
    return_data['market_cap'] = return_data['exchange_rate'] * return_data['circulating_supply']

    if return_data['realized_market_cap'] > 0:
        return_data['mvrv'] = return_data['market_cap'] / return_data['realized_market_cap']
    else:
        return_data['mvrv'] = 0

    return return_data


def _add_years(d, years):
    """Return a date that's `years` years after the date (or datetime)
    object `d`. Return the same calendar date (month and day) in the
    destination year, if it exists, otherwise use the following day
    (thus changing February 29 to March 1).

    """
    try:
        return d.replace(year = d.year + years)
    except ValueError:
        return d + (date(d.year + years, 1, 1) - date(d.year, 1, 1))


def get_data(symbol: str, date: datetime):

    date_string = date.strftime('%Y-%m-%d')

    with open(os.path.join(STORE_MARKET_DATA, symbol + '.csv')) as file:

        for line in file:

            line_split = line.split(',')

            if line_split[0] == date_string:
                return {
                    'date': date,
                    'circulating_supply': int(line_split[1]),
                    'not_moved_coins': int(line_split[2]),
                    'market_cap': float(line_split[3]),
                    'realized_market_cap': float(line_split[4]),
                    'mvrv': float(line_split[5]),
                    'coins_older_1y': int(line_split[6]),
                    'num_holder': int(line_split[7]),
                    'exchange_rate': float(line_split[8]),
                }
