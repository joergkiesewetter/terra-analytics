import config
from manage_transactions import update_token_transactions
from util import logging

log = logging.get_custom_logger(__name__, config.LOG_LEVEL)

if __name__ == "__main__":

        log.info('fetching new token transactions')
        update_token_transactions()

    #     if token['source_exchange_rates'] == 'coin_gecko':
    #         source_coin_gecko.update_exchange_rates(token['symbol'])
    #     elif token['source_exchange_rates'] == 'nexustracker':
    #         source_nexustracker.update_exchange_rates(token['symbol'])
    #
    #
    #     update_realized_market_capitalization(token)
    #     update_balances(token)
    #
    #     #
    #     # calculation of results
    #     #
    #
    #     calculate_realized_market_capitalization(token['symbol'])
    #     calculate_token_holder_stats(token)
    #
    #     log.debug('--------')

    # # postphone calculation of top token holders to have the other data faster
    # for token in config.TOKEN:
    #     calculate_top_token_holder(token)
    #
    #     if len(token['lending_contracts']) > 0:
    #         calculate_top_token_holder_normalized(token)
