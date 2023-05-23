import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))

from degiro_app import DataSet, Stocks

if __name__ == "__main__":
    path = "data/Account.csv"
    df = DataSet(path).data
    print(df)
    df_sum = Stocks(df=df).return_on_stock_complete("ALIBABA GROUP HOLDING")
    print(df_sum)
    df_total = Stocks(df=df).return_portfolio()
    print(df_total)
