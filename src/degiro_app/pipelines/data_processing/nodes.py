"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.18.8
"""

import pandas as pd
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional


# Node Auxiliary Class and Methods
@dataclass
class DataCols:
    """
    Defines the columns of the raw dataframe and associates it to an attribute
    """

    reg_date: str = "Fecha"
    reg_hour: str = "Hora"
    value_date: str = "Fecha valor"
    product: str = "Producto"
    isin: str = "ISIN"
    desc: str = "Descripción"
    curr_rate: str = "Tipo"
    varcur: str = "Variación"
    var: str = "Unnamed: 8"
    cashcur: str = "Saldo"
    cash: str = "Unnamed: 10"
    id_order: str = "ID Orden"
    action: str = "Action"
    number: str = "Number"
    price: str = "Price"
    pricecur: str = "Price Currency"


def split_string(string: str, cols: DataCols = DataCols()) -> pd.Series:
    """
    Splits a string and returns a pandas Series with a split
    - action: 'buy' or 'sell'
    - number: number of shares involved in the movement
    - price: price of product
    - pricecur: currency of product
    """
    mapping = {"Compra": "buy", "Venta": "sell"}
    if string.startswith("Compra") or string.startswith("Venta"):
        split_row = string.split("@")
        return pd.Series(
            {
                cols.action: mapping.get(split_row[0].split()[0]),
                cols.number: float(split_row[0].split()[1]),
                cols.price: float(split_row[1].split()[0].replace(",", ".")),
                cols.pricecur: split_row[1].split()[1],
            }
        )
    elif string.startswith("VENCIMIENTO"):
        split_row = string.split(": ")[1].split("@")
        return pd.Series(
            {
                cols.action: mapping.get(split_row[0].split()[0]),
                cols.number: float(split_row[0].split()[1]),
                cols.price: float(split_row[1].split()[0].replace(",", ".")),
                cols.pricecur: split_row[1].split()[1],
            }
        )
    else:
        return pd.Series(
            {
                cols.action: None,
                cols.number: None,
                cols.price: None,
                cols.pricecur: None,
            }
        )


def type_converter(df: pd.DataFrame, cols: DataCols = DataCols()) -> pd.DataFrame:
    """
    Defines type conversions of dataframe columns
    """
    df[cols.reg_date] = pd.to_datetime(df[cols.reg_date])
    df[cols.reg_hour] = pd.to_datetime(df[cols.reg_hour])
    df[cols.value_date] = pd.to_datetime(df[cols.value_date])
    if df[cols.var].dtype != float:
        df[cols.var] = pd.to_numeric(
            df[cols.var].str.replace(",", "."), errors="coerce"
        )
    if df[cols.cash].dtype != float:
        df[cols.cash] = pd.to_numeric(
            df[cols.cash].str.replace(",", "."), errors="coerce"
        )
    df[cols.desc] = df[cols.desc].astype(str)
    return df


def fix_orphan_ids(df: pd.DataFrame, cols: DataCols = DataCols()) -> pd.DataFrame:
    """
    Filters out all rows which are orphan (sometimes the order id is splitted into
    two columns)
    """
    length_idorder = len("da6f08da-40d6-437e-b721-cd3456f89950")
    df_fixed = df[
        (df[cols.id_order].notnull()) & (df[cols.id_order].str.len() < length_idorder)
    ]
    orphan_rows = []
    for _, row in df_fixed.iterrows():
        if row[cols.id_order][8] != "-":
            orphan_rows.append(row.name)
    for orphan_row in orphan_rows:
        df[cols.id_order].loc[orphan_row - 1] += df[cols.id_order].loc[orphan_row]
    df.drop(orphan_rows, inplace=True)
    return df


# -----------------------------------------
# -------------- Clean node ---------------
# -----------------------------------------
def split_description(df: pd.DataFrame, cols: DataCols = DataCols()) -> pd.DataFrame:
    df = type_converter(df)
    df = fix_orphan_ids(df)
    df[[cols.action, cols.number, cols.price, cols.pricecur]] = df[cols.desc].apply(
        split_string
    )
    return df


# -----------------------------------------
# ------------- Return node ---------------
# -----------------------------------------
def return_on_stock(
    df: pd.DataFrame,
    stock: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    cols: DataCols = DataCols(),
) -> float:
    df = type_converter(df)
    df = df[df[cols.product] == stock].copy()

    if start_date is not None:
        df = df[df[cols.value_date] >= start_date]
    if end_date is not None:
        df = df[df[cols.value_date] <= end_date]

    df["Amount"] = df[cols.number] * df[cols.price]
    df["Shares Sold"] = 0
    df["2M Conflict"] = False
    return_stock = 0

    df.sort_values(by=cols.value_date, inplace=True)

    for _, row in df.iterrows():
        if row[cols.action] == "sell":
            shares_to_sell = row[cols.number]
            shares_sold_so_far = 0
            shares_bought = []

            for _, buy_row in df.loc[
                (df[cols.action] == "buy") & (df["Shares Sold"] < df[cols.number])
            ].iterrows():
                shares_available = buy_row[cols.number] - buy_row["Shares Sold"]
                shares_sold = min(shares_to_sell - shares_sold_so_far, shares_available)
                shares_sold_so_far += shares_sold
                df.loc[df.index == buy_row.name, "Shares Sold"] += shares_sold
                shares_bought.append([shares_sold, buy_row[cols.price]])

                if shares_sold_so_far >= shares_to_sell:
                    break

            df.loc[df.index == row.name, "Shares Sold"] = shares_to_sell

            bought_amount = sum([x[0] * x[1] for x in shares_bought])
            sell_amount = row["Amount"]
            return_of_sale = sell_amount - bought_amount
            twomonth_limit = pd.Timestamp(row[cols.value_date] + timedelta(days=60))
            if (return_of_sale < 0) & (
                len(
                    df.loc[
                        (df[cols.action] == "buy")
                        & (df[cols.value_date] >= row[cols.value_date])
                        & (df[cols.value_date] <= twomonth_limit)
                    ]
                )
            ) > 0:
                pass
            else:
                return_stock += return_of_sale
    return return_stock


def return_on_stock_complete(
    df: pd.DataFrame,
    stock: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    cols: DataCols = DataCols(),
) -> pd.DataFrame:
    df_summary = pd.DataFrame(columns=df.columns)
    df = type_converter(df)
    df = df[df[cols.product] == stock].copy()

    if start_date is not None:
        df = df[df[cols.value_date] >= start_date]
    if end_date is not None:
        df = df[df[cols.value_date] <= end_date]

    df["Amount"] = df[cols.number] * df[cols.price]
    df["Shares Sold"] = 0
    df["2M Conflict"] = False
    return_stock = 0

    df.sort_values(by=cols.value_date, inplace=True)

    for _, row in df.iterrows():
        if row[cols.action] == "sell":
            row["Amount EUR"] = df[cols.var][
                (df[cols.id_order]) == row[cols.id_order]
            ].sum()
            df_summary = pd.concat([df_summary, pd.DataFrame([row])], ignore_index=True)
            shares_to_sell = row[cols.number]
            shares_sold_so_far = 0
            bought_amount = []

            for _, buy_row in df.loc[
                (df[cols.action] == "buy") & (df["Shares Sold"] < df[cols.number])
            ].iterrows():
                shares_available = buy_row[cols.number] - buy_row["Shares Sold"]
                shares_sold = min(shares_to_sell - shares_sold_so_far, shares_available)
                shares_sold_so_far += shares_sold
                df.loc[df.index == buy_row.name, "Shares Sold"] += shares_sold
                buy_row["Shares Sold"] = shares_sold
                buy_row["Amount EUR"] = (
                    df[cols.var][df[cols.id_order] == buy_row[cols.id_order]].sum()
                    * shares_sold
                    / buy_row["Number"]
                )
                bought_amount.append(buy_row["Amount EUR"])
                buy_row["Amount Cost EUR"] = df[cols.var][
                    (df[cols.id_order] == buy_row[cols.id_order])
                    & (df[cols.desc].str.contains("Costes"))
                ].sum()
                df_summary = pd.concat(
                    [df_summary, pd.DataFrame([buy_row])], ignore_index=True
                )

                if shares_sold_so_far >= shares_to_sell:
                    break

            df.loc[df.index == row.name, "Shares Sold"] = shares_to_sell

            bought_amount = sum([x for x in bought_amount])
            sell_amount = row["Amount"]
            return_of_sale = sell_amount - bought_amount
            twomonth_limit = pd.Timestamp(row[cols.value_date] + timedelta(days=60))
            if (return_of_sale < 0) & (
                len(
                    df.loc[
                        (df[cols.action] == "buy")
                        & (df[cols.value_date] >= row[cols.value_date])
                        & (df[cols.value_date] <= twomonth_limit)
                    ]
                )
            ) > 0:
                pass
            else:
                return_stock += return_of_sale
    return df_summary


def return_portfolio(
    df: pd.DataFrame,
    stock: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    cols: DataCols = DataCols(),
) -> pd.DataFrame:
    stock_list = df[cols.product][
        (df[cols.action].str.contains("buy")) | (df[cols.action].str.contains("sell"))
    ].unique()
    global_df = pd.concat([return_on_stock_complete(df, stock) for stock in stock_list])
    return global_df
