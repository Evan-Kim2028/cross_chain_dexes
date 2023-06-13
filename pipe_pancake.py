import asyncio
import itertools
from queryportal.subgraphinterface import SubgraphInterface
from datetime import datetime, timedelta
import os
import polars as pl
pl.Config.set_fmt_str_lengths(200)


# Load decentralized endpoints
sgi = SubgraphInterface(endpoints=[
    'https://api.thegraph.com/subgraphs/name/messari/pancakeswap-v3-bsc'
])

# make a folder called data
if not os.path.exists('data'):
    os.makedirs('data')

for subgraph in list(sgi.subject.subgraphs.keys()):
    os.makedirs(f'data/{subgraph}_raw', exist_ok=True)


########################################################
# Query params

# Fields to be returned from the query
query_paths = [
    'hash',
    'account_id',
    # 'to',
    # 'from',
    'blockNumber',
    'timestamp',
    'tokenIn_symbol',
    'tokenOut_symbol',
    'amountIn',
    'amountOut',
    'pool_id',
    'amountInUSD',
    'amountOutUSD'
]



query_size = 2500000

# ASYNC STUFF
def process_subgraph(subgraph, start_date, end_date):

    filter = {
        'timestamp_gte': int(start_date.timestamp()),
        'timestamp_lte': int(end_date.timestamp()),
    }

    sgi.query_entity(
        query_size=query_size,
        entity='swaps',
        name=subgraph,
        query_paths=query_paths,
        filter_dict=filter,
        orderBy='timestamp',
        # graphql_query_fmt=True,
        saved_file_name=f'data/{subgraph}_raw/{subgraph}_swaps_{start_date.strftime("%m-%d")}_{end_date.strftime("%m-%d")}'
        )

    print(f'queried {subgraph} from {start_date.strftime("%m-%d")} to {end_date.strftime("%m-%d")}')



async def main():
    subgraph_keys = list(sgi.subject.subgraphs.keys())
    date_ranges = [(start_date, start_date + timedelta(days=1)) for start_date in [datetime(2023, 5, 17) + timedelta(days=i) for i in range(0, 14, 1)]]

    await asyncio.gather(*[asyncio.to_thread(process_subgraph, subgraph, start_date, end_date) for subgraph, (start_date, end_date) in itertools.product(subgraph_keys, date_ranges)])


# Run the asyncio event loop
asyncio.run(main())
########################################################
