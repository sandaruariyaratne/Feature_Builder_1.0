import pandas as pd
import numpy as np

class FeatureValidator:

    def __init__(self, method='standard', correlation_threshold=0.95):
        self.method = method
        self.correlation_threshold = correlation_threshold

    def validate(self, df: pd.DataFrame) -> bool:

        print("Starting Feature Validation...")

        # =========================
        # 0. Empty check
        # =========================
        if df is None or df.empty:
            print("CRITICAL: Feature DataFrame is empty.")
            return False

        # =========================
        # 1. Validate ALL columns dynamically
        # =========================
        columns = df.columns.tolist()

        if len(columns) == 0:
            print("CRITICAL: No features found.")
            return False

        print(f"Validating {len(columns)} features: {columns}")

        # =========================
        # 2. Null check (ALL columns)
        # =========================
        null_count = df.isnull().sum().sum()
        if null_count > 0:
            print(f"CRITICAL: Found {null_count} null values.")
            return False

        # =========================
        # 3. Infinite values check (ALL columns)
        # =========================
        if np.isinf(df.select_dtypes(include=[np.number]).values).any():
            print("CRITICAL: Infinite values detected.")
            return False

        # =========================
        # 4. Range check (ALL numeric columns)
        # =========================
        numeric_df = df.select_dtypes(include=[np.number])

        if self.method == 'standard':
            if (numeric_df.abs() > 10).any().any():
                print("WARNING: Extreme Z-score values detected (>10)")

        elif self.method == 'minmax':
            if ((numeric_df < 0) | (numeric_df > 1)).any().any():
                print("WARNING: Values outside [0,1] range detected")

        # =========================
        # 5. Correlation check (ALL columns)
        # =========================
        if numeric_df.shape[1] > 1:

            corr_matrix = numeric_df.corr().abs()

            upper = corr_matrix.where(
                np.triu(np.ones(corr_matrix.shape), k=1).astype(bool)
            )

            to_drop = [
                col for col in upper.columns
                if any(upper[col] > self.correlation_threshold)
            ]

            if to_drop:
                print(f"INFO: Highly correlated features detected: {to_drop}")

        print("Validation Passed. Data is clean.")
        return True