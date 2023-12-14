from scripts.utils import connect_to_db, load_to_sql
from scripts.utils import get_last_record
from scripts.email import send_price_alert


def price_alert(config_file, whitelist):
    engine = connect_to_db(config_file, "redshift")
    if engine is not None:
        try:
            for wh in whitelist:

                df = get_last_record('fact_crypto_trading',
                                     engine, wh['coin_id'])

                if not df.empty and df['high'].iloc[0] >= wh['target_price']:
                    ticker = wh['ticker']
                    send_price_alert(f'Price Alert {ticker}')

        except Exception as e:
            print(f'Error in price alert {e}')
            raise e
