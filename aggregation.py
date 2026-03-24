import pandas as pd

class AggregationEngine:
    def __init__(self, functions=None):
        self.functions = functions or ['mean', 'max', 'min', 'std']

    def aggregate(self, windows) -> pd.DataFrame:

        print(f"Applying Aggregations: {self.functions}...")
    
        feature_rows = []
    
        for i, window in enumerate(windows):
    
            if window.empty:
                continue
    
            agg = window.agg(self.functions).T
    
            # Flatten column names (mean, max, etc.)
            agg.columns = [f"{stat}" for stat in agg.columns]
    
            # Convert to single row
            feature = agg.stack()
    
            feature.index = [
                f"{metric}_{stat}"
                for metric, stat in feature.index
            ]
    
            # ✅ ADD TIMESTAMP (window end time)
            feature.name = window.index.max()
    
            feature_rows.append(feature)
    
        if not feature_rows:
            print("⚠️ No valid windows to aggregate")
            return pd.DataFrame()
    
        features_df = pd.DataFrame(feature_rows)
    
        # ✅ Set index name
        features_df.index.name = "timestamp"
    
        print(f"Aggregation complete. Feature shape: {features_df.shape}")
    
        return features_df