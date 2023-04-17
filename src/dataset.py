"""Class to read dataset and define several basic functions"""
import pandas as pd


class DatasetProcessor:
    def __init__(self, path):
        self.path = path
        self.data = pd.read_csv(self.path)

    def replace_commas(self, col_name):
        self.data[col_name] = self.data[col_name].str.replace(',', '.')

    def convert_to_float(self, col_name):
        self.data[col_name] = self.data[col_name].astype(float)

    def drop_col(self, col_name):
        self.data.drop(col_name, axis=1, inplace=True)

    def rename_col(self, col_name, new_col_name):
        self.data.rename(columns = {col_name:new_col_name}, inplace=True)

    def drop_na(self, col_name):
        self.data.dropna(subset=[col_name], inplace=True)

    def split_string(string):
        if string.startswith('Compra') or string.startswith('Venta'):
            split_row = string.split("@")
            return pd.Series({
                'Action': split_row[0].split()[0],
                'Number': split_row[0].split()[1], 
                'Price': split_row[1].split()[0].replace(',', '.'),
                'Currency': split_row[1].split()[1]
            })
        else:
            return pd.Series({
                'Action': None,
                'Number': None, 
                'Price': None,
                'Currency': None
            })
        
    def apply_split_string_to_col(self, col_name):
        self.data[['Action', 'Number', 'Price', 'Currency']] = self.data[col_name].apply(self.split_string)

    def replace_string_in_col(self, col_name, string_1, string_2):
        self.data[col_name].replace(string_1, string_2, inplace=True)

    def date_and_hour_transform(self, date_col, hour_col):
        self.data['Date'] = pd.to_datetime(self.data[date_col] + ' ' + self.date[hour_col], format='%d-%m-%Y %H:%M')
        self.drop_col([date_col, hour_col])

    def save_data(self, out_path):
        self.data.to_csv(out_path, index=False)

