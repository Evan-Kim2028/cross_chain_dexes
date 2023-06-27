import polars as pl


def filter_swap_pairs(df_filepaths: str, token_0: str, token_0_decimal: int, token_1: str, token_1_decimal: int) -> pl.DataFrame:
    '''
    Filter data for a specific swap pair of tokens
    '''

    # scan for token 0 -> token 1 swap
    q_in = (
        pl.scan_parquet(df_filepaths, rechunk=True)
        .filter((pl.col("tokenIn_symbol") == token_0) & (pl.col("tokenOut_symbol") == token_1))
    )

    # convert query into df
    df_in = q_in.collect()

    # apply decimal converesion
    df_in = df_in.with_columns(
        amountIn= pl.col("amountIn") / 10 ** token_0_decimal,
        amountOut= pl.col("amountOut") / 10 ** token_1_decimal
    )

    # calculate execution price
    df_in = df_in.with_columns(
        executionPrice= pl.col("amountOut") / pl.col("amountIn")
    )

    # scan for token 1 -> token 0 swap
    q_out = (
        pl.scan_parquet(df_filepaths, rechunk=True)
        .filter((pl.col("tokenIn_symbol") == token_1) & (pl.col("tokenOut_symbol") == token_0))
    )

    # convert query into df
    df_out = q_out.collect()
    
    # apply decimal converesion
    df_out = df_out.with_columns(
        amountIn= pl.col("amountIn") / 10 ** token_1_decimal,
        amountOut= pl.col("amountOut") / 10 ** token_0_decimal
    )

    # calculate execution price
    df_out = df_out.with_columns(
        executionPrice= pl.col("amountIn") / pl.col("amountOut")
    )

    # concat dfs
    return pl.concat([df_in, df_out])