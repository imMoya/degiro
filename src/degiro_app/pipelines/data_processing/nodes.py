"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.18.8
"""

import os
import sys
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

import pandas as pd

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
)

from degiro_app import DataSet, Stocks


# Nodes from degiro_app
def node_dataset_clean(path: str) -> pd.DataFrame:
    return DataSet(path).data


def node_return_portfolio(df: pd.DataFrame) -> pd.DataFrame:
    return Stocks(df=df).return_portfolio()


# def return_dividends(
#     df: pd.DataFrame,
#     start_date: Optional[str] = None,
#     end_date: Optional[str] = None,
#     cols: DataCols = DataCols(),
# ) -> pd.DataFrame:
#     df = type_converter(df)
#     if start_date is not None:
#         df = df[df[cols.value_date] >= start_date]
#     if end_date is not None:
#         df = df[df[cols.value_date] <= end_date]
#     df = df[df[cols.desc].str.contains("ividendo")]
#     return df
