import pandas as pd
from scripts.binance import get_24hr_ticker
from scripts.utils import connect_to_db, load_to_sql
import uuid


def generate_df_general(whitelist):
    print(whitelist)
    try:
        pairs = [coin['ticker'] for coin in whitelist]
    except Exception as e:
        print(f"Error fetching pairs: {e}")
        raise e

    data_list = []
    for pair in pairs:
        try:
            print(f"Fetching data for {pair}")
            pair_data = get_24hr_ticker(pair)
            pair_data['ticker'] = pair
            data_list.append(pair_data)
        except Exception as e:
            print(f"Error fetching data for {pair}: {e}")
            raise e

    df = pd.DataFrame(data_list)
    return df


def generate_df_trades(df_original, whitelist):
    df = df_original.copy()
    df['exchange_id'] = 1
    df['coin_id'] = df['ticker'].map(
        {coin['ticker']: coin['coin_id'] for coin in whitelist})
    # Generate a new uuid
    df['trading_id'] = [str(uuid.uuid4()) for _ in range(len(df))]

    # Remove column ticker
    df.drop(columns=['ticker'], inplace=True)

    return df


def generate_df_date(df_original):
    df = df_original.copy()
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    unique_dates = df['date'].dt.normalize().unique()
    dates_df = pd.DataFrame(unique_dates, columns=['date'])

    dates_df['day'] = dates_df['date'].dt.day
    dates_df['month'] = dates_df['date'].dt.month
    dates_df['year'] = dates_df['date'].dt.year

    return dates_df


def insert_into_db(config_file, whitelist):
    engine = connect_to_db(config_file, "redshift")
    if engine is not None:
        df = generate_df_general(whitelist=whitelist)

        try:
            df_trades = generate_df_trades(df, whitelist=whitelist)
            load_to_sql(df_trades, 'fact_crypto_trading', engine, 'trading_id')
        except Exception as e:
            print(f"Error loading trades to database: {e}")
            raise e

        try:
            df_dates = generate_df_date(df)
            load_to_sql(df_dates, 'dim_dates', engine, 'date')
        except Exception as e:
            print(f"Error loading dates to database: {e}")
            raise e
