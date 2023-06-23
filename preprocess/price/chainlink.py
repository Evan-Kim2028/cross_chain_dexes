import polars as pl
import os

pl.Config.set_fmt_str_lengths(200)


if not os.path.exists('data/price'):
    os.makedirs('data/price')


price_filepaths = 'data/prices_raw/chainlink-prices-subgraph/*.parquet'


df = (
    pl.scan_parquet(price_filepaths)
).collect()

# sort by timestamp
df = df.sort('timestamp')

# convert timestamp to datetime
df = df.with_columns(
    pl.from_epoch("timestamp"),
    price= pl.col("price") / 10 ** 8,
    blockchain=pl.lit('chainlink')
)

# save df
df.write_parquet('data/price/chainlink_eth_price.parquet')