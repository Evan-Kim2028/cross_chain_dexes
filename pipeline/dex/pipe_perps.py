import asyncio
import itertools
from queryportal.subgraphinterface import SubgraphInterface
from datetime import datetime, timedelta
import os
import polars as pl
pl.Config.set_fmt_str_lengths(200)


# Load decentralized endpoints
sgi = SubgraphInterface(endpoints=[
    'https://api.thegraph.com/subgraphs/name/messari/gains-trade-arbitrum',
    'https://api.thegraph.com/subgraphs/name/messari/gmx-arbitrum'
])

# make a folder called data
if not os.path.exists('data'):
    os.makedirs('data')

for subgraph in list(sgi.subject.subgraphs.keys()):
    os.makedirs(f'data/{subgraph}', exist_ok=True)


########################################################
# Query params

# Fields to be returned from the query
query_paths = [
    'timestampOpened',
    'timestampClosed',
    'account_id',
    'balance',
    'balanceUSD',
    'blockNumberOpened',
    'blockNumberClosed',
    'closeBalanceUSD',
    'closeCollateralBalanceUSD',
    'collateralBalance',
    'collateralBalanceUSD',
    'leverage',
    'side',
    'realisedPnlUSD',
    'hashOpened',
    'hashClosed',
    'liquidation_profitUSD',
    'collateral_symbol',
    'asset_symbol',
    'collateralIn_outputToken_symbol',
    'collateralIn_inputToken_symbol',
    'collateralOut_outputToken_symbol',
    'collateralOut_inputToken_symbol',
]



query_size = 200000

# ASYNC STUFF
def process_subgraph(subgraph, start_date, end_date):

    filter = {
        'timestampOpened_gte': int(start_date.timestamp()),
        'timestampOpened_lte': int(end_date.timestamp()),
    }

    sgi.query_entity(
        query_size=query_size,
        entity='positions',
        name=subgraph,
        query_paths=query_paths,
        filter_dict=filter,
        orderBy='timestampOpened',
        # graphql_query_fmt=True,
        saved_file_name=f'data/{subgraph}/{subgraph}_positions_{start_date.strftime("%m-%d")}_{end_date.strftime("%m-%d")}'
        )

    print(f'queried {subgraph} from {start_date.strftime("%m-%d")} to {end_date.strftime("%m-%d")}')



async def main():
    subgraph_keys = list(sgi.subject.subgraphs.keys())
    date_ranges = [(start_date, start_date + timedelta(days=1)) for start_date in [datetime(2023, 5, 30) + timedelta(days=i) for i in range(0, 2, 1)]]    #jan 1 to June 1 is 152 days

    await asyncio.gather(*[asyncio.to_thread(process_subgraph, subgraph, start_date, end_date) for subgraph, (start_date, end_date) in itertools.product(subgraph_keys, date_ranges)])


# Run the asyncio event loop
asyncio.run(main())
########################################################

# 5.2gb 9:05am
# 5.6gb 11:41am