"Computes return on pandas dataframe containing stock movements"
import pandas as pd
from dataclasses import dataclass
from datetime import timedelta


@dataclass
class DataCols:
    name: str = "Name"
    desc: str = "Descripción"
    date: str = "Date"
    var: str = "Unmamed: 8"
    varcur: str = "Variación"
    cash: str = "Saldo"
    cashcur: str = "Unnamed: 10"
    action: str = "Action"
    number: str = "Number"
    price: str = "Price"
    pricecur: str = "Currency"
    shares_sold: str = "Shares Sold"


class ReturnCalculator:
    def __init__(self, data: pd.DataFrame, cols: type):
        self.data = data
        self.cols = cols

    def return_on_stock(self, stock: str, start_date=None, end_date=None):
        if start_date is not None:
            start_date = pd.to_datetime(start_date + "00:00", format="%d-%m-%Y%H:%M")
        if end_date is not None:
            end_date = pd.to_datetime(end_date + "23:59", format="%d-%m-%Y%H:%M")

        df = self.data[self.data[self.cols.name] == stock].copy()

        if start_date is not None:
            df = df[df[self.cols.date] >= start_date]
        if end_date is not None:
            df = df[df[self.cols.date] <= end_date]

        df["Amount"] = df[self.cols.number] * df[self.cols.price]
        df["Shares Sold"] = 0
        df["2M Conflict"] = False
        return_stock = 0

        df.sort_values(by=self.cols.date, inplace=True)

        for _, row in df.iterrows():
            if row[self.cols.action] == "sell":
                shares_to_sell = row[self.cols.number]
                shares_sold_so_far = 0
                shares_bought = []

                for _, buy_row in df.loc[
                    (df[self.cols.action] == "buy")
                    & (df["Shares Sold"] < df[self.cols.number])
                ].iterrows():
                    shares_available = (
                        buy_row[self.cols.number] - buy_row["Shares Sold"]
                    )
                    shares_sold = min(
                        shares_to_sell - shares_sold_so_far, shares_available
                    )
                    shares_sold_so_far += shares_sold
                    df.loc[df.index == buy_row.name, "Shares Sold"] += shares_sold
                    shares_bought.append([shares_sold, buy_row[self.cols.price]])

                    if shares_sold_so_far >= shares_to_sell:
                        break

                df.loc[df.index == row.name, "Shares Sold"] = shares_to_sell

                bought_amount = sum([x[0] * x[1] for x in shares_bought])
                sell_amount = row["Amount"]
                return_of_sale = sell_amount - bought_amount
                twomonth_limit = row[self.cols.date] + timedelta(days=60)
                if (return_of_sale < 0) & (
                    len(
                        df.loc[
                            (df[self.cols.action] == "buy")
                            & (df[self.cols.date] >= row[self.cols.date])
                            & (df[self.cols.date] <= twomonth_limit)
                        ]
                    )
                ) > 0:
                    pass
                else:
                    return_stock += return_of_sale
        return return_stock
