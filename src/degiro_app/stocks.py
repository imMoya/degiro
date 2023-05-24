from datetime import timedelta
from typing import Optional

import pandas as pd

from degiro_app.dataset import DataCols, DataSet


class Stocks:
    def __init__(
        self,
        path: Optional[str] = None,
        df: Optional[pd.DataFrame] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ):
        self.path = path
        self.df = DataSet(path).data if df is None else df
        self.start_date = start_date
        self.end_date = end_date

    def return_on_stock_complete(
        self,
        stock: str,
        cols: DataCols = DataCols(),
    ) -> pd.DataFrame:
        df_summary = pd.DataFrame(columns=self.df.columns)
        df = self.df[self.df[cols.product] == stock].copy()

        if self.start_date is not None:
            df = df[df[cols.value_date] >= self.start_date]
        if self.end_date is not None:
            df = df[df[cols.value_date] <= self.end_date]

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
                df_summary = pd.concat(
                    [df_summary, pd.DataFrame([row])], ignore_index=True
                )
                shares_to_sell = row[cols.number]
                shares_sold_so_far = 0
                bought_amount = []

                for _, buy_row in df.loc[
                    (df[cols.action] == "buy") & (df["Shares Sold"] < df[cols.number])
                ].iterrows():
                    shares_available = buy_row[cols.number] - buy_row["Shares Sold"]
                    shares_sold = min(
                        shares_to_sell - shares_sold_so_far, shares_available
                    )
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
                return_of_sale = sell_amount + bought_amount
                twomonth_limit = pd.Timestamp(row[cols.value_date] + timedelta(days=60))
                if (return_of_sale < 0) and (
                    len(
                        df.loc[
                            (df[cols.action] == "buy")
                            & (df[cols.value_date] >= row[cols.value_date])
                            & (df[cols.value_date] <= twomonth_limit)
                        ]
                    )
                ) > 0:
                    df_summary["2M Conflict"].loc[
                        df_summary[cols.id_order] == row[cols.id_order]
                    ] = True
                else:
                    return_stock += return_of_sale
        return df_summary

    def return_portfolio(
        self,
        cols: DataCols = DataCols(),
    ) -> pd.DataFrame:
        stock_list = self.df[cols.product][
            (self.df[cols.action].str.contains("buy"))
            | (self.df[cols.action].str.contains("sell"))
        ].unique()
        global_df = pd.concat(
            [self.return_on_stock_complete(stock) for stock in stock_list]
        )
        return global_df
