import config
from calculate_daily_payment_data import calculate_daily_payment_data
from calculate_daily_retention_data import calculate_daily_retention_data
from calculate_daily_transaction_data import calculate_daily_transaction_data
from calculate_market_data import calculate_market_data
from final_data_general import final_data_general
from final_data_payments import final_data_payments
from final_data_transactions import final_data_transactions
from final_data_user import final_data_user
from manage_realized_market_capitalization import update_realized_market_capitalization
from manage_transactions import update_token_transactions
from util import logging

log = logging.get_custom_logger(__name__, config.LOG_LEVEL)

if __name__ == "__main__":

    log.info('fetching new token transactions')
    update_token_transactions()
    update_realized_market_capitalization()

    #
    # calculate interim results
    #

    # structure ./raw/stats_daily_payments/<token>.csv
    calculate_daily_payment_data()

    calculate_daily_transaction_data()

    calculate_daily_retention_data()

    calculate_market_data()

    #
    # calculate final results to use in the frontend
    #
    final_data_general()
    final_data_payments()
    final_data_transactions()
    final_data_user()

    # structure ./raw/stats_total_amount_of_coins/<token>.csv
    # calculate_total_amount_of_coins()

    # - total amount of coins
    # - velocity of currency per day
    # see https://www.investopedia.com/articles/investing/091814/what-bitcoins-intrinsic-value.asp
    # -- M1: circulating supply
    # * - daily payments - total amount
    # * - daily payments - count
    # * - daily payment - average
    # * - daily transactions by type
    # * - daily new users (no transaction before)
    # * - user count total
    # * - daily active users (accounts with at least 1 transaction on this day)
    # * - daily payments (count) by address
    # * - daily payments (amount) by address
    # user_count/<currency>.csv
    # <date>,<count>
    # - rolling retention
    # - daily swap volume

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
