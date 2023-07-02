# The Perpetual Evolution of Dexs


# Concentrated Liquidity Volume
Subgraph endpoints are queried with `queryportal`, which can be installed with ```pip install git+https://github.com/Evan-Kim2028/subgraph-query-portal.git@v1.0.1```.
Note that querying ~ 3 months of data took about 3 days of constant uptime to query around 60m rows of data with a couple of crashes in between. If a crash occurs, the
date in the script can be changed to pick up where the last days worth of data was saved.

# Perp Margin Volume
Dune data is used to retrieve margin volume from the following queries and saved manually into a folder called `dune_perp_margin`. :
- [kwenta](https://dune.com/queries/1295704/2219432)
- [vertex](https://dune.com/queries/2478071/4081805)
- [perp](https://dune.com/queries/1295653/2219323)
- [gmx](https://dune.com/queries/1347021/2298368)
- [mux](https://dune.com/queries/1300339/2227806)
- [level](https://dune.com/queries/2238732/3670305)
- [gains](https://dune.com/queries/1295757/2219503)

# Pyth
Pyth does not have a subgraph or api to query historical data. Data was kindly provided asking in the discord for historical ETH price data. 

# Preprocessing
There are two preprocessing folders. `dex` preprocesses concentrated liquidity dex files and `price` preprocesses the chainlink files. Files that retrieved from subgraphs require extra preprocessing because they are broken down into daily files and need to be combined. 

# Charts
Jupyter notebooks used to create the charts are in the `chart_notebooks` folder. Each one can be run in independent order. Note that some charts are saved with a custom formatting library that is not included in this repo that uses a pre-defined color scheme and font from matplotlib. Charts are saved in the `charts` folder.

