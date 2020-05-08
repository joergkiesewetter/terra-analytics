import config
from calculate_daily_payment_data import calculate_daily_payment_data
from manage_transactions import update_token_transactions
from util import logging

log = logging.get_custom_logger(__name__, config.LOG_LEVEL)

if __name__ == "__main__":

    log.info('fetching new token transactions')
    update_token_transactions()

    # structure ./raw/stats_daily_payments/<token>.csv
    calculate_daily_payment_data()

    # structure ./raw/stats_total_amount_of_coins/<token>.csv
    # calculate_total_amount_of_coins()

    # - total amount of coins
    # - velocity of currency per day
    # * - daily payments - total amount
    # * - daily payments - count
    # - daily payment - average
    # - daily transactions by type
    # - daily new users (no transaction before)
    # - daily active users (accounts with at least 1 transaction on this day)
    #* - daily payments (count) by address
    #* - daily payments (amount) by address
    # - user count total
    # user_count/<currency>.csv
    # <date>,<count>
    #  - rolling retention

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
