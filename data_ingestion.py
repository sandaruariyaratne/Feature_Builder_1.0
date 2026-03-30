import pandas as pd
import requests
from typing import Dict, List


class DataIngestor:
    def __init__(self, api_url: str):
        self.api_url = api_url
        self.session = requests.Session()

        self.required_metrics: List[str] = [
            "cpu_usage_rate",
            "container_start_time_seconds",
            "node_cpu_total",
            "node_memory_MemAvailable_bytes",
            "latency_p95",
            "latency_std",
            "error_rate",
            "memory_usage",
            "net_throughput",
            "disk_io_rate"
        ]

    def ingest(self, payload: Dict) -> pd.DataFrame:
        try:
            print("Calling Data Retrieval API...")

            response = self.session.post(self.api_url, json=payload, timeout=30)
            response.raise_for_status()
            data = response.json()

            metrics_data = data.get("metrics", {})
            if not metrics_data:
                raise ValueError("No metrics found in response")

            dfs = []

            for metric_name, series_list in metrics_data.items():

                # ✅ Filter only required metrics
                if metric_name not in self.required_metrics:
                    continue

                if not series_list:
                    continue

                series_dfs = []

                for series in series_list:
                    values = series.get("values", [])

                    if not values:
                        continue

                    df = pd.DataFrame(values, columns=["timestamp", "value"])

                    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
                    df["value"] = pd.to_numeric(df["value"], errors="coerce")

                    df = df.set_index("timestamp")
                    series_dfs.append(df)

                if not series_dfs:
                    continue

                # 🔗 Combine multiple label series into one
                combined_df = pd.concat(series_dfs, axis=1)

                # 📊 Aggregate (mean across series)
                combined_df[metric_name] = combined_df.mean(axis=1)

                dfs.append(combined_df[[metric_name]])

            if not dfs:
                raise ValueError("No required metrics found")

            # 🔗 Merge all metrics
            final_df = pd.concat(dfs, axis=1).sort_index()

            # ⏱ Resample
            #final_df = final_df.resample("1min").mean()

            print(f"Ingestion complete ✅ Shape: {final_df.shape}")
            return final_df

        except Exception as e:
            print(f"Error: {e}")
            return pd.DataFrame()

