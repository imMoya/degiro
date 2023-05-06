"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.18.8
"""

import pandas as pd
from dataclasses import dataclass


@dataclass
class DataCols:
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


def type_converter(df: pd.DataFrame, cols=DataCols) -> pd.DataFrame:
    df[cols.reg_date] = pd.to_datetime(df[cols.reg_date], format="%d-%m-%Y")
    df[cols.reg_hour] = pd.to_datetime(df[cols.reg_hour], format="%H:%M").dt.time
    df[cols.value_date] = pd.to_datetime(df[cols.value_date], format="%d-%m-%Y")
    df[cols.var] = pd.to_numeric(df[cols.var].str.replace(",", "."), errors="coerce")
    df[cols.cash] = pd.to_numeric(df[cols.cash].str.replace(",", "."), errors="coerce")
    return df
