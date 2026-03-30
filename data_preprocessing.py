import pandas as pd
import numpy as np

class DataPreprocessor:
    def __init__(self):
        pass

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:

        if df.empty:
            raise ValueError("Preprocessor received an empty DataFrame.")

        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("DataFrame index must be DatetimeIndex")

        # 1️⃣ Ensure numeric
        df = df.apply(pd.to_numeric, errors='coerce')

        # 2️⃣ Sort by time
        df = df.sort_index()


        # 4️⃣ Resample to fixed 1-minute grid
        df = df.resample("1min").mean()

        # 5️⃣ Handle missing values
        df = df.ffill(limit=5)
        df = df.interpolate(method="linear", limit_area="inside")
        df = df.fillna(0)

        # 6️⃣ Drop constant columns
        #if not df.empty:
            #df = df.loc[:, (df != df.iloc[0]).any()]

        print(f"Preprocessing complete. Cleaned Shape: {df.shape}")
        print("\nPreprocessed Metrics:\n", df.head())

        return df