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

        '''
        # 3️⃣ Identify counter metrics
        counter_prefixes = [
            "node_cpu_seconds_total",
            "container_cpu_usage_seconds_total",
            "container_network_receive_bytes_total",
        ]

        counter_cols = [
            col for col in df.columns
            if any(col.startswith(prefix) for prefix in counter_prefixes)
        ]

        if counter_cols:

            # Calculate value differences
            value_diff = df[counter_cols].diff()

            # Calculate time difference (seconds)
            time_diff = df.index.to_series().diff().dt.total_seconds()

            # Convert counters to rate (per second)
            df[counter_cols] = value_diff.div(time_diff, axis=0)

            # Handle counter resets (negative values)
            df[counter_cols] = df[counter_cols].clip(lower=0) '''

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
        print("\nPreprocessed Metrics:\n", df.head(10))

        return df