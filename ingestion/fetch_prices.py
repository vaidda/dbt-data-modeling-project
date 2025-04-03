import os
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import logging
import time

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

COINS = os.getenv("COINS", "bitcoin,ethereum,solana").split(",")
DB_URI = os.getenv("DB_URI")

def fetch_prices():
    all_data = []
    engine = create_engine(DB_URI)

    with engine.begin() as conn:
        # Get last ingestion end_date per coin
        latest_dates = {}
        for coin in COINS:
            result = conn.execute(text("""
                SELECT MAX(end_date) FROM ingestion_runs WHERE coin_id = :coin
            """), {"coin": coin})
            last_date = result.scalar()
            latest_dates[coin] = last_date or (datetime.today().date() - pd.Timedelta(days=30))

    # Start fetching coin data
    for coin in COINS:
        start = latest_dates[coin]
        today = datetime.today().date()
        delta_days = (today - start).days

        if delta_days < 1:
            logging.info(f"No new data needed for {coin}")
            continue

        url = f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart"
        params = {
            'vs_currency': 'usd',
            'days': str(delta_days),
            'interval': 'daily'
        }

        for attempt in range(3):
            try:
                logging.info(f"Fetching {coin} (last={start}, now={today}, attempt={attempt+1})...")
                r = requests.get(url, params=params, timeout=10)
                r.raise_for_status()
                data = r.json()
                prices = data['prices']
                market_caps = data.get('market_caps', [])
                volumes = data.get('total_volumes', [])
                break
            except requests.exceptions.RequestException as e:
                logging.warning(f"{coin} fetch failed: {e}")
                time.sleep(2 ** attempt)
        else:
            logging.error(f"Failed to fetch {coin} after 3 attempts")
            continue

        if not prices:
            logging.warning(f"No prices returned for {coin}")
            continue

        # Build rows
        for i in range(len(prices)):
            ts = prices[i][0]
            all_data.append({
                'coin_id': coin,
                'timestamp': datetime.utcfromtimestamp(ts / 1000),
                'price_usd': prices[i][1],
                'market_cap': market_caps[i][1] if i < len(market_caps) else None,
                'volume': volumes[i][1] if i < len(volumes) else None
            })

    if not all_data:
        logging.warning("No data fetched. Exiting early.")
        return

    df = pd.DataFrame(all_data)

    with engine.begin() as conn:
        # Append only (no truncate to preserve history)
        df.to_sql('raw_crypto_prices', con=conn, if_exists='append', index=False)

        # Log ingestion_runs per coin
        for coin in COINS:
            coin_df = df[df['coin_id'] == coin]
            if coin_df.empty:
                continue

            start_date = coin_df['timestamp'].min().date()
            end_date = coin_df['timestamp'].max().date()
            rows_inserted = coin_df.shape[0]

            conn.execute(text("""
                INSERT INTO ingestion_runs (coin_id, start_date, end_date, rows_inserted)
                VALUES (:coin_id, :start_date, :end_date, :rows_inserted)
            """), {
                "coin_id": coin,
                "start_date": start_date,
                "end_date": end_date,
                "rows_inserted": rows_inserted
            })

    logging.info(f"✅ Ingested total {len(df)} rows across {len(df['coin_id'].unique())} coins.")

if __name__ == "__main__":
    fetch_prices()
