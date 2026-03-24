import pandas as pd
from sklearn.preprocessing import StandardScaler, MinMaxScaler

class TransformationLayer:
    def __init__(self, method='standard'):
        self.method = method.lower()

        if self.method == 'standard':
            self.scaler = StandardScaler()
        elif self.method == 'minmax':
            self.scaler = MinMaxScaler()
        else:
            raise ValueError("Method must be 'standard' or 'minmax'")

        self.is_fitted = False

    def fit(self, df: pd.DataFrame):
        if df.empty:
            return
        
        # Remove constant columns
        df = df.loc[:, df.std() != 0]

        self.scaler.fit(df)
        self.columns = df.columns  # store columns
        self.is_fitted = True

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        if not self.is_fitted:
            raise ValueError("Scaler must be fitted before transform()")

        print(f"Applying {self.method} transformation...")

        timestamps = df.index

        # Keep only fitted columns
        df = df[self.columns]

        scaled_values = self.scaler.transform(df)

        transformed_df = pd.DataFrame(
            scaled_values,
            index=timestamps,
            columns=self.columns
        )

        return transformed_df