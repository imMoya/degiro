import pandas as pd
from dataclasses import dataclass


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


class DataSet:
    def __init__(self, path, fix_orphan: bool = True):
        self.path = path
        self.data = self.type_converter(pd.read_csv(self.path))
        if fix_orphan == True:
            self.data = self.fix_orphan_ids(self.data)

    @staticmethod
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

    @staticmethod
    def fix_orphan_ids(df: pd.DataFrame, cols: DataCols = DataCols()) -> pd.DataFrame:
        """
        Filters out all rows which are orphan (sometimes the order id is splitted into
        two columns)
        """
        length_idorder = len("da6f08da-40d6-437e-b721-cd3456f89950")
        df_fixed = df[
            (df[cols.id_order].notnull())
            & (df[cols.id_order].str.len() < length_idorder)
        ]
        orphan_rows = []
        for _, row in df_fixed.iterrows():
            if row[cols.id_order][8] != "-":
                orphan_rows.append(row.name)
        for orphan_row in orphan_rows:
            df[cols.id_order].loc[orphan_row - 1] += df[cols.id_order].loc[orphan_row]
        df.drop(orphan_rows, inplace=True)
        return df
