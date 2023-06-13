# kwenta_pipe


# Pipeline
First install queryportal with ```pip install git+https://github.com/Evan-Kim2028/subgraph-query-portal.git@v1.0.0```.
All of the files that begin with `pipe_` are the pipeline files used to query differnt subgraphs for on-chain data. Note that querying ~ 3 months of data took about 3 days of constant uptime with a couple of crashes in between. If a crash occurs, the
date in the script can be changed to pick up where the last days worth of data was saved.

# Preprocessing
After the data is queried, there are two preprocess files. `preprocess_univ3.py` handles processing the uniswap v3 data and `preprocess_pancakeswapv3.py` handles processing the uniswap v2 data. The data is then saved to the `data` folder. Both of these
files are very similar, the main differences being the folder naming convention to keep univ3 and pancakeswap files separate.

# Charts
All of the `flow_` files are used to create the various charts. Each one can be run in independent order. Note that charts are saved with a custom formatting library that is not included in this repo. If you want to reproduce the charts, it will require a little bit of extra effort.

