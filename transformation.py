import pandas as pd
from sklearn.preprocessing import StandardScaler, MinMaxScaler

class TransformationLayer:
    def __init__(self, method=None):
        self.method = method.lower() if method else None

        if self.method == 'standard':
            self.scaler = StandardScaler()
        elif self.method == 'minmax':
            self.scaler = MinMaxScaler()
        elif self.method is None:
            self.scaler = None  # No scaling
        else:
            raise ValueError("Method must be 'standard', 'minmax', or None")

        self.is_fitted = False
        self.columns = None

    def fit(self, df: pd.DataFrame):
        if df.empty:
            return

        # Remove constant columns
        #df = df.loc[:, df.std() != 0]
        self.columns = df.columns

        # Only fit if scaler exists
        if self.scaler:
            self.scaler.fit(df)

        self.is_fitted = True

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        if not self.is_fitted:
            raise ValueError("Scaler must be fitted before transform()")

        timestamps = df.index

        # Keep only fitted columns
        df = df[self.columns]

        # ✅ If no method → return as-is
        if self.method is None:
            print("No transformation applied (raw features).")
            return df.copy()

        print(f"Applying {self.method} transformation...")

        scaled_values = self.scaler.transform(df)

        transformed_df = pd.DataFrame(
            scaled_values,
            index=timestamps,
            columns=self.columns
        )

        return transformed_df