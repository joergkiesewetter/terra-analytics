import math
import re
import time
import traceback
from datetime import datetime

import requests

import config
from util import logging

# TERRA_BASE_URL = 'https://fcd.terra.dev/v1/txs'
TERRA_BASE_URL = 'http://116.202.245.125:1317/txs'
# TERRA_BASE_URL = 'http://0.0.0.0:1317/txs'
TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

log = logging.get_custom_logger(__name__, config.LOG_LEVEL)

class Terra:

    @staticmethod
    def get_transaction(block: int):
        url = TERRA_BASE_URL + '?' + \
              '&limit=100' + \
              '&tx.height=' + str(block)
        # '&block=' + str(block)
        # when requesting the terracli rest-server, use:


        # request the first page
        response = None
        while not response:
            try:
                response = requests.get(url + '&page=1')
            except:
                traceback.print_exc()

        while response.status_code != 200:
            log.warning(response.status_code)
            response = requests.get(url + '&page=1')
            time.sleep(1)

        loaded_json = response.json()

        total_count = int(loaded_json.get('total_count') or loaded_json.get('totalCnt'))
        limit = int(loaded_json['limit'])

        raw_transactions = loaded_json['txs']

        # request all other pages
        for i in range(2, int(math.ceil(total_count/limit)) + 1):

            response = requests.get(url + '&page=' + str(i))

            while response.status_code != 200:
                log.warning(response.status_code)
                response = requests.get(url + '&page=' + str(i))
                time.sleep(1)

            loaded_json = response.json()
            raw_transactions.extend(loaded_json['txs'])

        final_transactions = list()

        for t in raw_transactions:

            # when the transaction has a code, there is an error. So, we just ignore the whole transaction
            if t.get('code'):
                continue

            #
            # add transaction for transaction fee
            #
            # TODO find out how to handle transaction fee (gas)
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
            tax_currency = ''
            gas = 0

            if 'fee' in t['tx']['value']:
                if len(t['tx']['value']['fee']['amount']) > 0:
                    tax_amount = int(t['tx']['value']['fee']['amount'][0]['amount'])
                    tax_currency = t['tx']['value']['fee']['amount'][0]['denom']
                gas = int(t['tx']['value']['fee']['gas'])

            #
            # get every transaction
            #
            for m in t['tx']['value']['msg']:

                if m['type'] == 'bank/MsgMultiSend':
                    # TODO support more than one currency exchange per transaction

                    total_amount = 0
                    for input in m['value']['inputs']:
                        total_amount += int(input['coins'][0]['amount'])

                    output_address = None
                    if len(m['value']['outputs']) == 1:
                        output_address = m['value']['outputs'][0]['address']

                    for i in range(len(m['value']['inputs'])):

                        amount = int(m['value']['inputs'][i]['coins'][0]['amount'])

                        final_transactions.append({
                            'block': int(t['height']),
                            'txhash': t['txhash'],
                            'timestamp': int(datetime.strptime(t['timestamp'], TIMESTAMP_FORMAT).timestamp()),
                            'type': m['type'],
                            'amount': amount,
                            'currency': m['value']['inputs'][i]['coins'][0]['denom'],
                            'from_address': m['value']['inputs'][i]['address'],
                            'to_address': output_address or m['value']['outputs'][i]['address'],
                            'tax_amount': int(round(tax_amount * (amount / total_amount))),
                            'tax_currency': tax_currency,
                        })

                elif m['type'] == 'bank/MsgSend':

                    final_transactions.append({
                        'block': int(t['height']),
                        'txhash': t['txhash'],
                        'timestamp': int(datetime.strptime(t['timestamp'], TIMESTAMP_FORMAT).timestamp()),
                        'type': m['type'],
                        'amount': int(m['value']['amount'][0]['amount']),
                        'currency': m['value']['amount'][0]['denom'],
                        'from_address': m['value']['from_address'],
                        'to_address': m['value']['to_address'],
                        'tax_amount': int(tax_amount),
                        'tax_currency': tax_currency,
                    })

                elif m['type'] == 'distribution/MsgWithdrawDelegationReward':

                    if len(t['logs'][0]['events'][0]['attributes']) == 4:
                        reward_from = t['logs'][0]['events'][0]['attributes'][0]['value']
                        delegation_reward = t['logs'][0]['events'][1]['attributes'][1]['value'].replace(',', ';')
                    else:
                        reward_from = ''
                        delegation_reward = '0uluna'

                    final_transactions.append({
                        'block': int(t['height']),
                        'txhash': t['txhash'],
                        'timestamp': int(datetime.strptime(t['timestamp'], TIMESTAMP_FORMAT).timestamp()),
                        'type': m['type'],
                        'delegator': m['value']['delegator_address'],
                        'validator': m['value']['validator_address'],
                        'reward_from': reward_from,
                        'delegation_reward': delegation_reward,
                        'tax_amount': int(tax_amount),
                        'tax_currency': tax_currency,
                    })

                elif m['type'] == 'distribution/MsgWithdrawValidatorCommission':

                    # TODO make the implementation better: respect the event type to find more complexe values
                    try:
                        commission_from = t['logs'][1]['events'][0]['attributes'][0]['value']
                    except IndexError:
                        commission_from = t['logs'][0]['events'][0]['attributes'][0]['value']

                    try:
                        commission = t['logs'][1]['events'][1]['attributes'][1]['value'].replace(',', ';')
                    except IndexError:
                        commission = t['logs'][0]['events'][1]['attributes'][1]['value'].replace(',', ';')

                    final_transactions.append({
                        'block': int(t['height']),
                        'txhash': t['txhash'],
                        'timestamp': int(datetime.strptime(t['timestamp'], TIMESTAMP_FORMAT).timestamp()),
                        'type': m['type'],
                        'validator': m['value']['validator_address'],
                        'commission_from': commission_from,
                        'commission': commission,
                    })


                elif m['type'] == 'gov/MsgDeposit':

                    final_transactions.append({
                        'block': int(t['height']),
                        'txhash': t['txhash'],
                        'timestamp': int(datetime.strptime(t['timestamp'], TIMESTAMP_FORMAT).timestamp()),
                        'type': m['type'],
                        'depositor': m['value']['depositor'],
                        'proposal_id': m['value']['proposal_id'],
                        'amount': m['value']['amount'][0]['amount'],
                        'currency': m['value']['amount'][0]['denom'],
                        'tax_amount': int(tax_amount),
                        'tax_currency': tax_currency,
                    })

                elif m['type'] == 'gov/MsgSubmitProposal':

                    proposal_id = -1

                    for event in t['events']:
                        if event['type'] == 'submit_proposal':
                            proposal_id = event['attributes'][0]['value']

                    if len(m['value']['initial_deposit']) > 0:
                        init_deposit_amount = m['value']['initial_deposit'][0]['amount']
                        init_deposit_currency = m['value']['initial_deposit'][0]['denom']
                    else:
                        init_deposit_amount = 0
                        init_deposit_currency = ''

                    final_transactions.append({
                        'block': int(t['height']),
                        'txhash': t['txhash'],
                        'timestamp': int(datetime.strptime(t['timestamp'], TIMESTAMP_FORMAT).timestamp()),
                        'type': m['type'],
                        'proposer': m['value']['proposer'],
                        'init_deposit_amount':  init_deposit_amount,
                        'init_deposit_currency': init_deposit_currency,
                        'proposal_id': proposal_id,
                        'proposal_title': m['value']['content']['value']['title'],
                        'proposal_text': m['value']['content']['value']['description'],
                        'tax_amount': int(tax_amount),
                        'tax_currency': tax_currency,
                    })

                elif m['type'] == 'market/MsgSwap':

                    ask_address = ''
                    ask_amount = -1
                    ask_currency = ''

                    bid_address = ''
                    bid_amount = -1
                    bid_currency = ''

                    swap_fee_amount = 0
                    swap_fee_currency = 'uluna'

                    for event in t['events']:
                        if event['type'] == 'swap':

                            ask_address = m['value']['trader']
                            ask_amount = m['value']['offer_coin']['amount']
                            ask_currency = m['value']['offer_coin']['denom']

                            bid_address = event['attributes'][1]['value']
                            bid_amount, bid_currency = _split_amount_currency(event['attributes'][2]['value'])
                            # TODO store swap_fee_amount and swap_fee_currency
                            swap_fee_amount, swap_fee_currency = _split_amount_currency(event['attributes'][3]['value'])

                    final_transactions.append({
                        'block': int(t['height']),
                        'txhash': t['txhash'],
                        'timestamp': int(datetime.strptime(t['timestamp'], TIMESTAMP_FORMAT).timestamp()),
                        'type': m['type'],
                        'ask_address': ask_address,
                        'ask_amount': ask_amount,
                        'ask_currency': ask_currency,
                        'bid_address': bid_address,
                        'bid_amount': bid_amount,
                        'bid_currency': bid_currency,
                        'swap_fee_amount': swap_fee_amount,
                        'swap_fee_currency': swap_fee_currency,
                        'tax_amount': int(tax_amount),
                        'tax_currency': tax_currency,
                    })

                elif m['type'] == 'oracle/MsgDelegateFeedConsent':
                    final_transactions.append({
                        'block': int(t['height']),
                        'txhash': t['txhash'],
                        'timestamp': int(datetime.strptime(t['timestamp'], TIMESTAMP_FORMAT).timestamp()),
                        'type': m['type'],
                    })

                elif m['type'] == 'oracle/MsgExchangeRateVote':
                    final_transactions.append({
                        'block': int(t['height']),
                        'txhash': t['txhash'],
                        'timestamp': int(datetime.strptime(t['timestamp'], TIMESTAMP_FORMAT).timestamp()),
                        'type': m['type'],
                        'exchange_rate': float(m['value']['exchange_rate']),
                        'currency': m['value']['denom'],
                        'feeder': m['value']['feeder'],
                        'tax_amount': int(tax_amount),
                        'tax_currency': tax_currency,
                    })

                elif m['type'] == 'oracle/MsgExchangeRatePrevote':
                    final_transactions.append({
                        'block': int(t['height']),
                        'txhash': t['txhash'],
                        'timestamp': int(datetime.strptime(t['timestamp'], TIMESTAMP_FORMAT).timestamp()),
                        'type': m['type'],
                        'currency': m['value']['denom'],
                        'feeder': m['value']['feeder'],
                        'tax_amount': int(tax_amount),
                        'tax_currency': tax_currency,
                    })

                elif m['type'] == 'staking/MsgBeginRedelegate':
                    final_transactions.append({
                        'block': int(t['height']),
                        'txhash': t['txhash'],
                        'timestamp': int(datetime.strptime(t['timestamp'], TIMESTAMP_FORMAT).timestamp()),
                        'type': m['type'],
                        'amount': m['value']['amount']['amount'],
                        'currency': m['value']['amount']['denom'],
                        'delegator_address': m['value']['delegator_address'],
                        'validator_dst_address': m['value']['validator_dst_address'],
                        'validator_src_address': m['value']['validator_src_address'],
                        'tax_amount': int(tax_amount),
                        'tax_currency': tax_currency,
                    })

                elif m['type'] == 'staking/MsgCreateValidator':
                    final_transactions.append({
                        'block': int(t['height']),
                        'txhash': t['txhash'],
                        'timestamp': int(datetime.strptime(t['timestamp'], TIMESTAMP_FORMAT).timestamp()),
                        'type': m['type'],
                        'pubkey': m['value']['pubkey'],
                        'amount': m['value']['value']['amount'],
                        'currency': m['value']['value']['denom'],
                        'commission_rate': m['value']['commission']['rate'],
                        'commission_max_rate': m['value']['commission']['max_rate'],
                        'commission_max_change_rate': m['value']['commission']['max_change_rate'],
                        'details': m['value']['description']['details'],
                        'moniker': m['value']['description']['moniker'],
                        'website': m['value']['description']['website'],
                        'identity': m['value']['description']['identity'],
                        'min_self_delegation': m['value']['min_self_delegation'],
                        'delegator': m['value']['delegator_address'],
                        'validator': m['value']['validator_address'],
                        'tax_amount': int(tax_amount),
                        'tax_currency': tax_currency,
                    })

                elif m['type'] == 'staking/MsgDelegate':
                    final_transactions.append({
                        'block': int(t['height']),
                        'txhash': t['txhash'],
                        'timestamp': int(datetime.strptime(t['timestamp'], TIMESTAMP_FORMAT).timestamp()),
                        'type': m['type'],
                        'delegator': m['value']['delegator_address'],
                        'validator': m['value']['validator_address'],
                        'amount': m['value']['amount']['amount'],
                        'currency': m['value']['amount']['denom'],
                        'tax_amount': int(tax_amount),
                        'tax_currency': tax_currency,
                    })

                elif m['type'] == 'staking/MsgEditValidator':
                    final_transactions.append({
                        'block': int(t['height']),
                        'txhash': t['txhash'],
                        'timestamp': int(datetime.strptime(t['timestamp'], TIMESTAMP_FORMAT).timestamp()),
                        'type': m['type'],
                        'address': m['value']['address'],
                        'details': m['value']['Description']['details'],
                        'moniker': m['value']['Description']['moniker'],
                        'website': m['value']['Description']['website'],
                        'identity': m['value']['Description']['identity'],
                        'commission_rate': m['value']['commission_rate'],
                        'min_self_delegation': m['value']['min_self_delegation'],
                        'tax_amount': int(tax_amount),
                        'tax_currency': tax_currency,
                    })

                elif m['type'] == 'staking/MsgUndelegate':
                    final_transactions.append({
                        'block': int(t['height']),
                        'txhash': t['txhash'],
                        'timestamp': int(datetime.strptime(t['timestamp'], TIMESTAMP_FORMAT).timestamp()),
                        'type': m['type'],
                        'delegator': m['value']['delegator_address'],
                        'validator': m['value']['validator_address'],
                        'amount': m['value']['amount']['amount'],
                        'currency': m['value']['amount']['denom'],
                        'tax_amount': int(tax_amount),
                        'tax_currency': tax_currency, })

                    print('message undelegate')

                else:
                    log.warning('transaction type not known: ' + m['type'])
                    raise NotImplementedError

        return final_transactions


# TODO make it better
def _split_amount_currency(value):

    if value.endswith('uluna'):
        return value[:-5], value[-5:]
    else:
        return value[:-4], value[-4:]
