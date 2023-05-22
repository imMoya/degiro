import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))

import pandas as pd

from degiro_app import DataSet

if __name__ == "__main__":
    path = "data/Account.csv"
    df = DataSet(path).data
    print(df)
