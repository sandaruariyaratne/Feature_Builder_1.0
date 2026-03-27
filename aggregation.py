import pandas as pd

class AggregationEngine:
    def __init__(self):
        pass

    def aggregate(self, windows) -> pd.DataFrame:

        print("Calculating custom features...")

        feature_rows = []

        for window in windows:

            if window.empty or len(window) < 2:
                continue

            # Sort by time (important)
            window = window.sort_index()

            feature = {}

            # 9. memory_growth_rate
            mem = window['container_memory_usage_bytes']
            time_diff = (window.index[-1] - window.index[-2]).total_seconds()

            if time_diff > 0:
                feature['memory_growth_rate'] = (mem.iloc[-1] - mem.iloc[-2]) / time_diff
            else:
                feature['memory_growth_rate'] = 0

            # 10. restart_flag
            start_time = window['container_start_time_seconds']
            feature['restart_flag'] = int(start_time.iloc[-1] < start_time.iloc[-2])

            # 11. memory_pressure
            mem_usage = window['container_memory_usage_bytes'].iloc[-1]
            node_mem = window['node_memory_MemAvailable_bytes'].iloc[-1]

            if node_mem > 0:
                feature['memory_pressure'] = mem_usage / node_mem
            else:
                feature['memory_pressure'] = 0

            # 12. cpu_container_vs_node_ratio
            cpu_container = window['container_cpu_usage_seconds_total']
            cpu_node = window['node_cpu_seconds_total']

            time_diff_full = (window.index[-1] - window.index[0]).total_seconds()

            if time_diff_full > 0:
                rate_container = (cpu_container.iloc[-1] - cpu_container.iloc[0]) / time_diff_full
                rate_node = (cpu_node.iloc[-1] - cpu_node.iloc[0]) / time_diff_full

                feature['cpu_container_vs_node_ratio'] = (
                    rate_container / rate_node if rate_node > 0 else 0
                )
            else:
                feature['cpu_container_vs_node_ratio'] = 0

            # 13. failure_streak
            probe = window['probe_success']
            
            streak = 0
            for val in reversed(probe.tolist()):   # <- convert to list
                if val == 0:
                    streak += 1
                else:
                    break
            
            feature['failure_streak'] = streak

            # Timestamp (window end)
            feature['timestamp'] = window.index.max()

            feature_rows.append(feature)

        if not feature_rows:
            print("⚠️ No valid windows")
            return pd.DataFrame()

        features_df = pd.DataFrame(feature_rows)
        features_df.set_index('timestamp', inplace=True)

        print(f"Feature extraction complete. Shape: {features_df.shape}")

        # ✅ PRINT FEATURES
        #print("\n🔍 Feature Data (first 5 rows):")
        #print(features_df.head())

        # Optional: print full data (be careful if large)
        # print("\nFull Feature Data:")
        # print(features_df)

        return features_df