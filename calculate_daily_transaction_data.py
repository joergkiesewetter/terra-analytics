import os
import traceback
from datetime import datetime, timedelta

import config
from manage_transactions import get_first_transaction_timestamp, get_transaction_data
from util import logging

# structure /terra-data/raw/stats_daily_transaction/<type>/<token>.csv
STORE_DAILY_TRANSACTIONS_DIRECTORY = '/terra-data/v2/raw/stats_daily_transactions'

# structure /terra-data/raw/stats_daily_address_payments/<token>/<date>.csv
STORE_DAILY_ADDRESS_PAYMENTS_DIRECTORY = '/terra-data/v2/raw/stats_daily_address_payments'

log = logging.get_custom_logger(__name__, config.LOG_LEVEL)


def calculate_daily_transaction_data():
    # symbol = token['symbol']
    # symbol_file = STORE_DIRECTORY + symbol

    os.makedirs(STORE_DAILY_TRANSACTIONS_DIRECTORY, exist_ok=True)

    max_time = datetime.utcnow()
    max_time = max_time.replace(hour=0, minute=0, second=0, microsecond=0)

    stop_processing = False

    date_to_process = get_first_transaction_timestamp()
    date_last_processed = _get_last_processed_date()
    date_to_process = max(date_to_process, date_last_processed + timedelta(days=1))

    log.debug('calculate: total amount of transactions per coin per type')

    # TODO remove all lines from STORE_DAILY_PAYMENTS_DIRECTORY which are in the future from date_to_process
    # TODO remove all files from STORE_DAILY_ADDRESS_PAYMENTS_DIRECTORY which are in the future from date_to_process

    if date_to_process >= max_time:
        return

    # with open(symbol_file, 'a') as file:
    while not stop_processing:

        log.debug('analysing transaction data for ' + date_to_process.strftime('%Y-%m-%d'))

        transactions = get_transaction_data(date_to_process)

        types = dict()

        for transaction in transactions:

            type = transaction[0]
            block = transaction[1]
            timestamp = transaction[2]
            tx_hash = transaction[3]

            if type not in types.keys():
                types[type] = {
                    'count': 0,
                    'currencies': dict(),
                }

            currency = None

            if type == 'bank_MsgMultiSend':
                currency = transaction[5]
            elif type == 'bank_MsgSend':
                currency = transaction[5]
            elif type == 'distribution_MsgWithdrawDelegationReward':
                currency = None
            elif type == 'distribution_MsgWithdrawValidatorCommission':
                currency = None
            elif type == 'gov_MsgDeposit':
                currency = transaction[7]
            elif type == 'gov_MsgSubmitProposal':
                currency = None
            elif type == 'market_MsgSwap':
                currency = None
            elif type == 'oracle_MsgDelegateFeedConsent':
                currency = None
            elif type == 'oracle_MsgExchangeRatePrevote':
                currency = transaction[5]
            elif type == 'oracle_MsgExchangeRateVote':
                currency = transaction[5]
            elif type == 'staking_MsgCreateValidator':
                currency = transaction[6]
            elif type == 'staking_MsgDelegate':
                currency = transaction[7]
            elif type == 'staking_MsgEditValidator':
                currency = None

            if currency and currency not in types[type]['currencies']:
                types[type]['currencies'][currency] = {
                    'count': 0,
                }

            if currency:
                types[type]['currencies'][currency]['count'] += 1
            else:
                types[type]['count'] += 1

        # print(types)

        for type in types.keys():

            os.makedirs(os.path.join(STORE_DAILY_TRANSACTIONS_DIRECTORY, type), exist_ok=True)

            if len(types[type]['currencies']) > 0:

                for currency in types[type]['currencies']:

                    with open(os.path.join(STORE_DAILY_TRANSACTIONS_DIRECTORY, type, currency + '.csv'), 'a') as file:
                        file.write(','.join([date_to_process.strftime('%Y-%m-%d'),
                                             str(types[type]['currencies'][currency]['count']),
                                            ]) + '\n')
            else:
                with open(os.path.join(STORE_DAILY_TRANSACTIONS_DIRECTORY, type, 'default.csv'), 'a') as file:
                    file.write(','.join(
                        [date_to_process.strftime('%Y-%m-%d'),
                         str(types[type]['count']),
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
    directories = [f for f in os.listdir(STORE_DAILY_TRANSACTIONS_DIRECTORY) if
             os.path.isdir(os.path.join(STORE_DAILY_TRANSACTIONS_DIRECTORY, f))]

    last_file_timestamp = datetime.strptime('1970-01-01', '%Y-%m-%d')

    for directory in directories:

        target_dir = os.path.join(STORE_DAILY_TRANSACTIONS_DIRECTORY, directory)

        files = [f for f in os.listdir(target_dir) if os.path.isfile(os.path.join(target_dir, f))]

        # get the file with the highest timestamp
        for file in files:
            symbol_file = os.path.join(target_dir, file)

            with open(symbol_file, 'r') as file:

                for line in file:
                    line_parts = line.split(',')

                    this_timestamp = datetime.strptime(line_parts[0], '%Y-%m-%d')

                    last_file_timestamp = max(last_file_timestamp, this_timestamp)

    return last_file_timestamp


def get_data(date, transaction_name):

    target_dir = os.path.join(STORE_DAILY_TRANSACTIONS_DIRECTORY, transaction_name)

    files = [f for f in os.listdir(target_dir) if os.path.isfile(os.path.join(target_dir, f))]

    return_data = {}

    for filename in files:
        try:

            token_name = filename.split('.')[0]

            with open(os.path.join(target_dir, filename), 'r') as file:

                for line in file:

                    if len(line) <= 0:
                        continue

                    line_parts = line.split(',')
                    line_date = datetime.strptime(line_parts[0], '%Y-%m-%d')

                    if line_date == date:
                        return_data[token_name] = {
                            'count': int(line_parts[1]),
                        }
                        break

        except:
            log.debug('error fetching data')
            traceback.print_exc()

    return return_data


def get_user(date):
    last_file_timestamp = None

    directories = [f for f in os.listdir(STORE_DAILY_ADDRESS_PAYMENTS_DIRECTORY) if os.path.isdir(os.path.join(STORE_DAILY_ADDRESS_PAYMENTS_DIRECTORY, f))]

    return_data = {}

    for dir in directories:

        token_dir = os.path.join(STORE_DAILY_ADDRESS_PAYMENTS_DIRECTORY, dir)
        filename = date.strftime('%Y-%m-%d')
        file_path = os.path.join(token_dir, filename + '.csv')

        if not os.path.isfile(file_path):
            continue

        return_data[dir] = []

        files = [f for f in os.listdir(token_dir) if os.path.isfile(os.path.join(token_dir, f))]

        # get the file with the highest timestamp
        with open(file_path, mode='r') as file:

            for line in file:
                line_split = line.split(',')
                return_data[dir].append({
                    'address': line_split[0],
                    'amount': line_split[1],
                    'count': line_split[2],
                })

    return return_data
