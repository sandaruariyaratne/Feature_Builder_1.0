import pandas as pd

class WindowingEngine:

    def __init__(self, window_size="5min"):
        self.window_size = window_size

    def create_windows(self, df: pd.DataFrame):

        if df.empty:
            return []

        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("Index must be DatetimeIndex")

        df = df.sort_index()

        # =========================
        # TUMBLING WINDOWS (FIXED)
        # =========================

        windows = [group for _, group in df.resample(self.window_size)]

        return windows