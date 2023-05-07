"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.18.8
"""

import pandas as pd
from dataclasses import dataclass


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


def type_converter(df: pd.DataFrame, cols=DataCols) -> pd.DataFrame:
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


# Node Functions used
def split_description(df: pd.DataFrame, cols=DataCols) -> pd.DataFrame:
    df = type_converter(df)
    df[[cols.action, cols.number, cols.price, cols.pricecur]] = df[cols.desc].apply(
        split_string
    )
    return df
