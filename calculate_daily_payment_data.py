import os
from datetime import datetime, timedelta

import config
from manage_transactions import get_first_transaction_timestamp, get_transaction_data
from util import logging

# structure /data/raw/terra/stats_daily_payments/<token>.csv
STORE_DAILY_PAYMENTS_DIRECTORY = '/data/raw/terra/stats_daily_payments'

# structure /data/raw/terra/stats_daily_address_payments/<token>/<date>.csv
STORE_DAILY_ADDRESS_PAYMENTS_DIRECTORY = '/data/raw/terra/stats_daily_address_payments'

log = logging.get_custom_logger(__name__, config.LOG_LEVEL)


def calculate_daily_payment_data():
    # symbol = token['symbol']
    # symbol_file = STORE_DIRECTORY + symbol

    os.makedirs(STORE_DAILY_PAYMENTS_DIRECTORY, exist_ok=True)

    max_time = datetime.utcnow()
    max_time = max_time.replace(hour=0, minute=0, second=0, microsecond=0)

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

        data = get_transaction_data(date_to_process, type_filter=['bank_MsgMultiSend', 'bank_MsgSend'])

        token = dict()

        for datum in data:

            type = datum[0]
            block = datum[1]
            timestamp = datum[2]
            tx_hash = datum[3]
            currency = datum[5]

            if currency not in token.keys():
                token[currency] = {
                    # 'file': open(os.path.join(STORE_DAILY_PAYMENTS_DIRECTORY, currency + '.csv'), 'a'),
                    'total_amount': 0,
                    'payment_count': 0,
                    'active_users': dict(),
                    # 'filename': None
                }

            amount = int(datum[4])
            from_address = datum[6]
            to_address = datum[7]
            tax_amount = int(datum[8])
            # tax_currency = datum[9]

            token[currency]['payment_count'] += 1
            token[currency]['total_amount'] += amount

            if from_address not in token[currency]['active_users'].keys():
                token[currency]['active_users'][from_address] = {
                    'total_amount': 0,
                    'payment_count': 0,
                }

            token[currency]['active_users'][from_address]['total_amount'] += amount
            token[currency]['active_users'][from_address]['payment_count'] += 1

        print(token)

        for currency in token.keys():

            with open(os.path.join(STORE_DAILY_PAYMENTS_DIRECTORY, currency + '.csv'), 'a') as file:
                file.write(','.join([date_to_process.strftime('%Y-%m-%d'),
                                     str(token[currency]['total_amount']),
                                     str(token[currency]['payment_count']),
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

        #
        #     if datum[0] in token['token_contracts'] or datum[0] in token['lending_contracts']:
        #         token_contracts_balance += int(datum[1])
        #
        #     elif datum[0] in token['team_accounts']:
        #         team_balance += int(datum[1])
        #
        #     elif datum[0] in known_addresses.exchange_addresses:
        #         exchange_balance += int(datum[1])
        #
        #     else:
        #         remaining_accounts.append({'account': datum[0], 'balance': int(datum[1]), })
        #
        # remaining_accounts.sort(key=lambda element: element['balance'], reverse=True)
        #
        # top20 = list()
        # top50 = list()
        # top100 = list()
        # top200 = list()
        # retail = list()
        #
        # i = 0
        # for account in remaining_accounts:
        #
        #     if i < 20:
        #         top20.append(account)
        #     elif i < 50:
        #         top50.append(account)
        #     elif i < 100:
        #         top100.append(account)
        #     elif i < 200:
        #         top200.append(account)
        #     else:
        #         retail.append(account)
        #
        #     i += 1
        #
        # date_string = date_to_process.strftime('%Y-%m-%d')
        # result = {
        #     'date': date_string,
        #     'token_contracts_balance': token_contracts_balance,
        #     'team_balance': team_balance,
        #     'exchanges_balance': exchange_balance,
        #     'top20': functools.reduce(lambda a, b: a + b['balance'], top20, 0),
        #     'top50': functools.reduce(lambda a, b: a + b['balance'], top50, 0),
        #     'top100': functools.reduce(lambda a, b: a + b['balance'], top100, 0),
        #     'top200': functools.reduce(lambda a, b: a + b['balance'], top200, 0),
        #     'retail': functools.reduce(lambda a, b: a + b['balance'], retail, 0), }
        #
        # file.write(result['date'] + ',' + str((result['token_contracts_balance'] / pow(10, 18))) + ',' + str(
        #     (result['team_balance'] / pow(10, 18))) + ',' + str(
        #     (result['exchanges_balance'] / pow(10, 18))) + ',' + str((result['top20'] / pow(10, 18))) + ',' + str(
        #     (result['top50'] / pow(10, 18))) + ',' + str((result['top100'] / pow(10, 18))) + ',' + str(
        #     (result['top200'] / pow(10, 18))) + ',' + str((result['retail'] / pow(10, 18))) + '\n')
        # file.flush()
        #
        # log.debug('calculate_token_holder_stats for ' + date_string)
        #
        # date_to_process += timedelta(days=1)
        #
        # if date_to_process >= max_time:
        #     stop_processing = True


def _get_last_processed_date():
    files = [f for f in os.listdir(STORE_DAILY_PAYMENTS_DIRECTORY) if os.path.isfile(os.path.join(STORE_DAILY_PAYMENTS_DIRECTORY, f))]

    last_file_timestamp = '1970-01-01'

    # get the file with the highest timestamp
    for file in files:
        symbol_file = os.path.join(STORE_DAILY_PAYMENTS_DIRECTORY, file)

        with open(symbol_file, 'r') as file:

            for line in file:
                line_parts = line.split(',')

                last_file_timestamp = line_parts[0]

    return datetime.strptime(last_file_timestamp, '%Y-%m-%d')


# def _get_data_to_process(date):
#     try:
#         with open(os.path.join(STORE_DIRECTORY, symbol, date.strftime('%Y-%m-%d') + '.csv'), 'rt') as file:
#
#             return_data = []
#
#             for line in file:
#                 return_data.append(line.split(';'))
#
#             return return_data
#     except:
#         return []
