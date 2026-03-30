from data_ingestion import DataIngestor
from data_preprocessing import DataPreprocessor
from windowing import WindowingEngine
from aggregation import AggregationEngine
from transformation import TransformationLayer
from validation import FeatureValidator


class FeatureBuilderPipeline:

    def __init__(self):

        self.ingestor = DataIngestor(
            api_url="http://127.0.0.1:8000/api/v1/metrics/retrieval"
        )

        self.preprocessor = DataPreprocessor()

        self.window_engine = WindowingEngine(
            window_size="5min"
        )

        self.agg_engine = AggregationEngine()

        self.transformer = TransformationLayer(method=None)

        self.validator = FeatureValidator()

    def run(self, payload: dict):

        print("\n🚀 Starting Pipeline...\n")

        # 1. INGESTION
        raw_data = self.ingestor.ingest(payload)
        print("Ingested:", raw_data.shape)

        # 2. PREPROCESSING
        clean_data = self.preprocessor.clean(raw_data)
        print("Preprocessed:", clean_data.shape)

        # 3. WINDOWING
        windows = self.window_engine.create_windows(clean_data)
        print("Windows:", len(windows))

        if not windows:
            raise ValueError("No windows created")

        # 4. AGGREGATION
        features = self.agg_engine.aggregate(windows)
        print("Features:", features.shape)

        # 5. TRANSFORMATION
        self.transformer.fit(features)
        scaled = self.transformer.transform(features)

        # 6. VALIDATION
        if not self.validator.validate(scaled):
            raise ValueError("Validation failed")

        print("\n✅ Pipeline Completed!")

        # =========================
        # ✅ 7. CONVERT TO JSON
        # =========================

        # move index (timestamp) to column
        scaled = scaled.reset_index()

        # rename index column properly
        scaled.rename(columns={"index": "timestamp"}, inplace=True)

        # convert timestamp to ISO format
        scaled["timestamp"] = scaled["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        # convert to list of dicts
        json_output = scaled.to_dict(orient="records")

        return json_output