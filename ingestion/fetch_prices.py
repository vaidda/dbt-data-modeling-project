import os
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

COINS = ['bitcoin', 'ethereum', 'solana']
DB_URI = os.getenv("DB_URI")

def fetch_prices():
    all_data = []
    for coin in COINS:
        url = f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart"
        params = {'vs_currency': 'usd', 'days': '30', 'interval': 'daily'}
        r = requests.get(url, params=params)
        prices = r.json()['prices']
        for [ts, price] in prices:
            all_data.append({
                'coin_id': coin,
                'timestamp': datetime.utcfromtimestamp(ts / 1000),
                'price_usd': price
            })

    df = pd.DataFrame(all_data)
    engine = create_engine(DB_URI)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE raw_crypto_prices"))
        df.to_sql('raw_crypto_prices', con=conn, if_exists='append', index=False)
    print(f"Ingested {len(df)} rows.")

if __name__ == "__main__":
    fetch_prices()
