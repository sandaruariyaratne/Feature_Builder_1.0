import pandas as pd
from datetime import timedelta

class WindowingEngine:

    def __init__(self, window_size="1h", stride="15min", type="sliding"):
        self.window_size = window_size
        self.stride = stride
        self.type = type.lower()

    def apply_horizon(self, df: pd.DataFrame, horizon_hours: int) -> pd.DataFrame:

        if df.empty:
            return df

        df = df.sort_index()

        latest_time = df.index.max()
        cutoff_time = latest_time - timedelta(hours=horizon_hours)

        return df[df.index >= cutoff_time]

    def create_windows(self, df: pd.DataFrame):

        if df.empty:
            return None

        df = df.sort_index()

        # Tumbling windows (non-overlapping)
        if self.type == "tumbling":

            return df.resample(self.window_size)

        # Sliding windows
        elif self.type == "sliding":

            windows = []

            start = df.index.min()
            end = df.index.max()

            window_delta = pd.Timedelta(self.window_size)
            stride_delta = pd.Timedelta(self.stride)

            current = start

            while current + window_delta <= end:

                window_df = df[current: current + window_delta]
                windows.append(window_df)

                current += stride_delta

            return windows

        else:
            raise ValueError("Type must be 'sliding' or 'tumbling'")