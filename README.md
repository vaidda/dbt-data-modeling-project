# Cryptocurrency Analytics Pipeline (DBT + Postgres + Superset)

This project ingests cryptocurrency price data from the CoinGecko API, models it using [dbt](https://www.getdbt.com/), and visualizes it in [Apache Superset](https://superset.apache.org/). It's fully containerized with Docker.

## Features

- Automated data ingestion from CoinGecko
- PostgreSQL as source-of-truth
- dbt modeling (staging → intermediate → marts)
- Superset dashboards (Top coins, price changes, trends)
- `.env`-based config
- Portable with Docker
