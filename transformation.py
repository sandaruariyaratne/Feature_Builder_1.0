import pandas as pd
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from aggregation import AggregationEngine

class TransformationLayer:

    def __init__(self, method=None):

        self.method = method.lower() if isinstance(method, str) else None

        if self.method == 'standard':
            self.scaler = StandardScaler()
        elif self.method == 'minmax':
            self.scaler = MinMaxScaler()
        elif self.method is None:
            self.scaler = None
        else:
            raise ValueError("Method must be 'standard', 'minmax', or None")

        self.is_fitted = False

    def fit(self, df: pd.DataFrame):

        if df.empty or self.scaler is None:
            return

        df = df.select_dtypes(include=['number'])
        self.scaler.fit(df)
        self.is_fitted = True

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:

        # =========================
        # ✅ CRITICAL FIX
        # =========================
        if self.method is None:
            return df  # 👈 EXACT OUTPUT FROM AGGREGATION LAYER

        if df.empty:
            return df

        if not self.is_fitted:
            raise ValueError("Scaler must be fitted before transform()")

        print(f"Applying {self.method} transformation...")

        timestamps = df.index

        numeric_df = df.select_dtypes(include=['number'])

        scaled = self.scaler.transform(numeric_df)

        transformed_df = pd.DataFrame(
            scaled,
            index=timestamps,
            columns=numeric_df.columns
        )

        # attach non-numeric columns back (if any)
        non_numeric = df.drop(columns=numeric_df.columns, errors='ignore')

        return pd.concat([transformed_df, non_numeric], axis=1)