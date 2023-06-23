import polars as pl
import os

pl.Config.set_fmt_str_lengths(200)

# `preprocess_dex.py` is used combine daily swaps for pancakeswapv3 into parquet files for bsc data.


dfs_agg = []


for file in os.listdir(f'data/pancakeswap-v3-bsc_raw'):
    # print(f'{file}: {os.path.getsize(f"data/{file}") / 1e6} MB')
    daily_df = pl.read_parquet(f'data/pancakeswap-v3-bsc_raw/{file}')
    # force `amountIn` column to `f64` type\
    daily_df = daily_df.with_columns(
        daily_df['amountIn'].cast(pl.Float64),
        daily_df['amountOut'].cast(pl.Float64)
    )

    # print(f'df size: {df.shape}')
    dfs_agg.append(daily_df)


# concat all daily_dfs
dexes = pl.concat(dfs_agg)

# get values to the right of - in subfolder name
blockchain = 'bsc'

#add subfolder column name
dexes = dexes.with_columns(pl.lit(blockchain).alias('blockchain'))

# sort by timestamp
dexes = dexes.sort('timestamp')

# convert timestamp to datetime
dexes = dexes.with_columns(
    pl.from_epoch("timestamp")
)

# save dexes to dex_swaps folder
dexes.write_parquet(f'data/univ3/pancakev3_bsc.parquet')


