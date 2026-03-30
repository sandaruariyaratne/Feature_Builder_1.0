import pandas as pd

class AggregationEngine:

    def __init__(self):
        pass

    def aggregate(self, windows) -> pd.DataFrame:

        print("Calculating features + attaching raw metrics...")

        feature_rows = []

        RAW_METRICS = [
            "latency_p95",
            "latency_std",
            "error_rate",
            "cpu_usage_rate",
            "memory_usage",
            "net_throughput",
            "disk_io_rate"
        ]

        for window in windows:

            if window.empty or len(window) < 2:
                continue

            window = window.sort_index()

            feature = {}

            # =========================
            # 1. ENGINEERED FEATURES
            # =========================

            # memory growth rate
            mem = window['memory_usage']
            time_diff = (window.index[-1] - window.index[-2]).total_seconds()

            feature['memory_growth_rate'] = (
                (mem.iloc[-1] - mem.iloc[-2]) / time_diff
                if time_diff > 0 else 0
            )

            # restart flag
            feature['restart_flag'] = int(
                window['container_start_time_seconds'].nunique() > 1
            )

            # memory pressure
            mem_usage = window['memory_usage'].iloc[-1]
            node_mem = window['node_memory_MemAvailable_bytes'].iloc[-1]

            feature['memory_pressure'] = mem_usage / (node_mem + 1e-9)

            # cpu ratio
            cpu_container = window['cpu_usage_rate']
            cpu_node = window['node_cpu_total']

            time_diff_full = (window.index[-1] - window.index[0]).total_seconds()

            if time_diff_full > 0:
                rate_container = (cpu_container.iloc[-1] - cpu_container.iloc[0]) / time_diff_full
                rate_node = (cpu_node.iloc[-1] - cpu_node.iloc[0]) / time_diff_full

                feature['cpu_container_vs_node_ratio'] = (
                    rate_container / rate_node if rate_node > 0 else 0
                )
            else:
                feature['cpu_container_vs_node_ratio'] = 0

            # failure streak
            probe = window['error_rate']
            streak = 0

            for val in reversed(probe.tolist()):
                if val > 0.1:
                    streak += 1
                else:
                    break

            feature['failure_streak'] = streak

            # =========================
            # 2. RAW METRICS (NO CALC)
            # =========================

            for col in RAW_METRICS:
                if col in window.columns:
                    feature[col] = window[col].iloc[-1]   # last observed value
                else:
                    feature[col] = None

            # timestamp
            feature['timestamp'] = window.index.max()

            feature_rows.append(feature)

        if not feature_rows:
            print("⚠️ No valid windows")
            return pd.DataFrame()

        df = pd.DataFrame(feature_rows)
        df.set_index('timestamp', inplace=True)

        print(f"Feature extraction complete. Shape: {df.shape}")

        return df