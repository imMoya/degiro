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


def map_string(string: str, mapping: dict) -> str:
    """
    Replaces a string through a mapping
    """
    return "".join(mapping.get(c, c) for c in string)


def split_string(string: str, cols=DataCols) -> pd.Series:
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
                cols.action: map_string(split_row[0].split()[0], mapping),
                cols.number: split_row[0].split()[1],
                cols.price: split_row[1].split()[0].replace(",", "."),
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


def type_converter(df: pd.DataFrame, cols: DataCols = DataCols) -> pd.DataFrame:
    """
    Defines type conversions of dataframe columns
    """
    df[cols.reg_date] = pd.to_datetime(df[cols.reg_date], format="%d-%m-%Y")
    df[cols.reg_hour] = pd.to_datetime(df[cols.reg_hour], format="%H:%M").dt.time
    df[cols.value_date] = pd.to_datetime(df[cols.value_date], format="%d-%m-%Y")
    df[cols.var] = pd.to_numeric(df[cols.var].str.replace(",", "."), errors="coerce")
    df[cols.cash] = pd.to_numeric(df[cols.cash].str.replace(",", "."), errors="coerce")
    df[cols.desc] = df[cols.desc].astype(str)
    return df


# -----------------------------------------
# -------------- Clean node ---------------
# -----------------------------------------
def split_description(df: pd.DataFrame, cols=DataCols) -> pd.DataFrame:
    df = type_converter(df)
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
    cols: DataCols = DataCols,
) -> float:
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
            twomonth_limit = row[cols.value_date] + timedelta(days=60)
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
