import pandas as pd
import numpy as np

class FeatureValidator:
    def __init__(self, method='standard', correlation_threshold=0.95):
        self.method = method
        self.correlation_threshold = correlation_threshold

        # ✅ Keep only these features
        self.required_columns = [
            'memory_growth_rate',
            'restart_flag',
            'memory_pressure',
            'cpu_container_vs_node_ratio',
            'failure_streak'
        ]

    def validate(self, df: pd.DataFrame) -> bool:
        print("Starting Feature Validation...")

        # ✅ Filter only required columns
        df = df[self.required_columns]

        # 0. Empty check
        if df.empty:
            print("CRITICAL: Feature DataFrame is empty.")
            return False

        # 1. Null check
        null_count = df.isnull().sum().sum()
        if null_count > 0:
            print(f"CRITICAL: Found {null_count} null values.")
            return False

        # 2. Infinite values check
        if np.isinf(df.values).any():
            print("CRITICAL: Infinite values detected.")
            return False

        # 3. Range check
        if self.method == 'standard':
            if (df.abs() > 10).any().any():
                print("WARNING: Extreme Z-score values detected (>10)")
        elif self.method == 'minmax':
            if ((df < 0) | (df > 1)).any().any():
                print("WARNING: Values outside [0,1] range detected")

        # 4. Correlation check
        if df.shape[1] < 300:
            corr_matrix = df.corr().abs()
            upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
            to_drop = [col for col in upper.columns if any(upper[col] > self.correlation_threshold)]

            if to_drop:
                print(f"INFO: High correlation in {len(to_drop)} features")

        print("Validation Passed. Data is clean.")
        return True