import pandas as pd
from scripts.utils import connect_to_db, load_to_sql


def seedExchanges(config_file, whitelist):
    engine = connect_to_db(config_file, "redshift")
    # Convert dic to df
    df = pd.DataFrame(whitelist)
    load_to_sql(df, 'dim_exchanges', engine, 'exchange_id')
    return


def seedCoins(config_file, whitelist):
    engine = connect_to_db(config_file, "redshift")
    df = pd.DataFrame(whitelist)
    load_to_sql(df, 'dim_coins', engine, 'coin_id')
    return
