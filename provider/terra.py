import math
import re
from datetime import datetime

import requests

import config
from util import logging

TERRA_BASE_URL = 'https://fcd.terra.dev/v1/txs'
TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

log = logging.get_custom_logger(__name__, config.LOG_LEVEL)

class Terra:

    @staticmethod
    def get_transaction(block: int):
        url = TERRA_BASE_URL + '?' + \
              'chainId=columbus-3' + \
              '&block=' + str(block)

        # request the first page
        response = requests.get(url + '&page=1')
        loaded_json = response.json()

        total_count = loaded_json['totalCnt']
        limit = loaded_json['limit']

        raw_transactions = loaded_json['txs']

        # request all other pages
        for i in range(2, int(math.ceil(total_count/limit)) + 1):

            response = requests.get(url + '&page=' + str(i))
            loaded_json = response.json()
            raw_transactions.extend(loaded_json['txs'])

        final_transactions = list()

        for t in raw_transactions:

            #
            # add transaction for transaction fee
            #
            # TODO find out how to handle transaction fee
            # for fee in t['tx']['value']['fee']['amount']:
            #     final_transactions.append({
            #         'block': int(t['height']),
            #         'txhash': t['txhash'],
            #         'timestamp': int(datetime.strptime(t['timestamp'], TIMESTAMP_FORMAT).timestamp()),
            #         'type': 'system/transactionFee',
            #         'amount': int(fee['amount']),
            #         'currency': fee['denom'],
            #     })

            #
            # get taxes
            #
            tax_amount = 0
            tax_currency = None

            for log_entry in t['logs']:

                if 'tax' not in log_entry['log']:
                    continue

                tax = log_entry['log']['tax']

                if tax and len(tax) > 0:
                    tax_amount = int(tax[:-4])
                    tax_currency = tax[-4:]

            #
            # get every transaction
            #
            for m in t['tx']['value']['msg']:

                if m['type'] == 'oracle/MsgExchangeRateVote':
                    final_transactions.append({
                        'block': int(t['height']),
                        'txhash': t['txhash'],
                        'timestamp': int(datetime.strptime(t['timestamp'], TIMESTAMP_FORMAT).timestamp()),
                        'type': m['type'],
                        'exchange_rate': float(m['value']['exchange_rate']),
                        'currency': m['value']['denom'],
                    })

                elif m['type'] == 'oracle/MsgExchangeRatePrevote':
                    final_transactions.append({
                        'block': int(t['height']),
                        'txhash': t['txhash'],
                        'timestamp': int(datetime.strptime(t['timestamp'], TIMESTAMP_FORMAT).timestamp()),
                        'type': m['type'],
                        'currency': m['value']['denom'],
                    })

                elif m['type'] == 'bank/MsgMultiSend':
                    # TODO support more than one currency exchange per transaction

                    total_amount = 0
                    for input in m['value']['inputs']:
                        total_amount += int(input['coins'][0]['amount'])

                    for i in range(len(m['value']['inputs'])):

                        amount = int(m['value']['inputs'][i]['coins'][0]['amount'])

                        final_transactions.append({
                            'block': int(t['height']),
                            'txhash': t['txhash'],
                            'timestamp': int(datetime.strptime(t['timestamp'], TIMESTAMP_FORMAT).timestamp()),
                            'type': m['type'],
                            'amount': amount,
                            'currency': m['value']['inputs'][i]['coins'][0]['denom'],
                            'from_address': m['value']['outputs'][i]['address'],
                            'to_address': m['value']['inputs'][i]['address'],
                            'tax_amount': int(round(tax_amount *  (amount / total_amount))),
                            'tax_currency': tax_currency,
                        })

                else:
                    log.warning('transaction type not known: ' + m['type'])

        return final_transactions