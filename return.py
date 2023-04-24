import pandas as pd
from dataclasses import dataclass

@dataclass
class DataCols:
    name: str='Name'
    desc: str='Descripción'
    date: str='Date'
    var: str='Unmamed: 8'
    varcur: str='Variación'
    cash: str='Saldo'
    cashcur: str='Unnamed: 10'
    action: str='Action'
    number: str='Number'
    price: str='Price'
    pricecur: str='Currency'
    shares_sold: str='Shares Sold'

class ReturnCalculator:
    def __init__(self, data: pd.DataFrame, cols: type):
        self.data = data
        self.cols = cols

    def return_on_stock(self, stock: str):
        df = self.data[self.data[self.cols.name] = stock].copy()
        df['Amount'] = df[self.cols.number] * df[self.cols.price]
        df['Shares Sold'] = 0

        df.sort_values(by=self.cols.date, inplace=True)

        for _, row in df.iterrows():
            if row[self.cols.action] == 'sell':
                shares_to_sell = row[self.cols.number]
                shares_sold_so_far = 0
                
                for _, buy_row in df.loc[(df[self.cols.action] == 'buy') & (df['Shares Sold'] < df[self.cols.number])].iterrows():
                    shares_available = buy_row[self.cols.number] - buy_row['Shares Sold']
                    shares_sold = min(shares_to_sell - shares_sold_so_far, shares_available)
                    shares_sold_so_far += shares_sold
                    df.loc[df.index == buy_row.name, 'Shares Sold'] += shares_sold
                    
                    if shares_sold_so_far >= shares_to_sell:
                        break
                        
                df.loc[df.index == row.name, 'Shares Sold'] = shares_to_sell

        # calculate total amount invested and received
        total_invested = df[(df[self.cols.action] == 'buy') & (df['Shares Sold'] > 0)]['Amount'].sum()
        total_received = df[(df[self.cols.action] == 'sell') & (df['Shares Sold'] > 0)]['Amount'].sum()
        return_stock = total_received - total_invested
        return return_stock



