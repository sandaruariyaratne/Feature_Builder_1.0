import pandas as pd
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Union

class DataIngestor:
    def __init__(self, source: str, base_url: str):
        """
        :param source: 'prometheus' or 'thanos'
        :param base_url: The API endpoint (e.g., http://prometheus:9090)
        """
        self.source = source.lower()
        self.base_url = base_url.rstrip('/')
        self.query_api = f"{self.base_url}/api/v1/query_range"
        self.session = requests.Session()
        
        # Your predefined 20 metrics
        self.metrics = [
            "node_cpu_seconds_total",
            "node_load1",
            "node_context_switches_total",
            "node_memory_MemAvailable_bytes",
            "node_filesystem_size_bytes",
            "node_memory_MemUsed_bytes",
            "node_memory_SwapUsed_bytes",
            "node_disk_io_time_seconds_total",
            "node_disk_read_bytes_total",
            "node_disk_written_bytes_total",
            "node_network_transmit_bytes_total",
            "node_network_receive_bytes_total",
            "node_network_receive_errs_total",
            "container_cpu_usage_seconds_total",
            "container_memory_usage_bytes",
            "container_memory_working_set_bytes",
            "container_network_receive_bytes_total",
            "probe_duration_seconds", "probe_success"
            # ... add all 20 metrics here
        ]

    def _fetch_single_metric(self, metric: str, start: int, end: int, step: str) -> pd.DataFrame:
        """Internal helper to fetch a single metric from the API."""
        params = {
            'query': metric,
            'start': start,
            'end': end,
            'step': step
        }

        
        
        try:
            response = self.session.get(self.query_api, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            results = data.get('data', {}).get('result', [])
            if not results:
                print(f"Warning: No data found for {metric}")
                return pd.DataFrame(columns=['timestamp', metric])

            # Prometheus returns [timestamp, value] pairs
            # We take the first result series (assuming no high-cardinality labels)
            #values = results[0]['values']
            #df = pd.DataFrame(values, columns=['timestamp', metric])

            dfs = []

            for series in results:
                labels = series["metric"]
                label_str = "_".join([f"{k}_{v}" for k,v in labels.items() if k != "__name__"])
                
                metric_name = f"{metric}_{label_str}" if label_str else metric
                
                df = pd.DataFrame(series["values"], columns=["timestamp", metric_name])
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
                df[metric_name] = df[metric_name].astype(float)
                
                dfs.append(df.set_index("timestamp"))

            return pd.concat(dfs, axis=1)
            
            # Convert to appropriate types
           # df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
            #df[metric] = df[metric].astype(float)
            
            #return df.set_index('timestamp')
            
        except Exception as e:
            print(f"Error fetching {metric}: {e}")
            return pd.DataFrame()

    def ingest(self, horizon_hours: int, step: str = "1m") -> pd.DataFrame:
        """
        Executes parallel ingestion of all 20 metrics.
        :param horizon_hours: How many hours back to look.
        :param step: Resolution (e.g., '1m', '15s', '5m').
        """
        now = datetime.now()
        end_time = int(now.timestamp())
        start_time = int((now - timedelta(hours=horizon_hours)).timestamp())

        print(f"Initiating ingestion from {self.source.upper()}...")

        # Execute fetches in parallel (20 threads for 20 metrics)
        with ThreadPoolExecutor(max_workers=min(10, len(self.metrics))) as executor:
            futures = [
                executor.submit(self._fetch_single_metric, m, start_time, end_time, step) 
                for m in self.metrics
            ]
            
            results = []
            for f in futures:
                r = f.result()
                if not r.empty:
                    results.append(r)

        if not results:
            raise ValueError("No data could be retrieved for any metrics.")

        # Join all metrics on the timestamp index
        # We use an 'outer' join to preserve all time points; 
        # the Preprocessing layer will handle the resulting NaNs.
        final_df = pd.concat(results, axis=1).sort_index()
        final_df = final_df.resample("1min").mean()
        
        print(f"Ingestion complete. Shape: {final_df.shape}")
        return final_df

# --- Usage Example ---
# ingestor = DataIngestor(source="prometheus", base_url="http://localhost:9090")
# raw_data = ingestor.ingest(horizon_hours=6, step="1m")

if __name__ == "__main__":
    ingestor = DataIngestor(
        source="prometheus",
        base_url="http://16.16.70.92:9090"
    )

    raw_data = ingestor.ingest(horizon_hours=1, step="1m")

    print(raw_data.head(10))
