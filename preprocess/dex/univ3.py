import polars as pl
import os

pl.Config.set_fmt_str_lengths(200)

# `preprocess_dex.py` is used combine daily swaps for univ3 into parquet files separated by blockchain.

# make a data/univ3 folder
if not os.path.exists('data/univ3'):
    os.makedirs('data/univ3')


dfs_agg = []

for subfolder in ['uniswap-v3-arbitrum', 'uniswap-v3-ethereum', 'uniswap-v3-optimism', 'uniswap-v3-polygon']:
    dfs_agg = []
    for file in os.listdir(f'data/univ3_raw/{subfolder}'):
        # print(f'{file}: {os.path.getsize(f"data/{subfolder}/{file}") / 1e6} MB')
        daily_df = pl.read_parquet(f'data/univ3_raw/{subfolder}/{file}')
        # print(f'df size: {df.shape}')
        dfs_agg.append(daily_df)


    # concat all daily_dfs
    dexes = pl.concat(dfs_agg)

    # get values to the right of - in subfolder name
    blockchain = subfolder.split('-')[-1]

    #add subfolder column name
    dexes = dexes.with_columns(pl.lit(blockchain).alias('blockchain'))

    # sort by timestamp
    dexes = dexes.sort('timestamp')

    # convert timestamp to datetime
    dexes = dexes.with_columns(
        pl.from_epoch("timestamp")
    )
    
    # save dexes to dex_swaps folder
    dexes.write_parquet(f'data/univ3/univ3_{blockchain}.parquet')


# read all 4 dex files in data/univ3 and concat them into a single file
dfs_agg = []
for file in os.listdir(f'data/univ3'):
    # print(f'{file}: {os.path.getsize(f"data/{subfolder}/{file}") / 1e6} MB')
    daily_df = pl.read_parquet(f'data/univ3/{file}')
    # print(f'df size: {df.shape}')
    dfs_agg.append(daily_df)



