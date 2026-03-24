from data_ingestion import DataIngestor
from data_preprocessing import DataPreprocessor
from windowing import WindowingEngine
from aggregation import AggregationEngine
from transformation import TransformationLayer
from validation import FeatureValidator


class FeatureBuilderPipeline:

    def __init__(self):
        self.ingestor = DataIngestor(
            source="prometheus",
            base_url="http://16.16.70.92:9090"
        )

        self.preprocessor = DataPreprocessor()

        self.window_engine = WindowingEngine(
            window_size="30min",
            stride="5min",
            type="sliding"
        )

        self.agg_engine = AggregationEngine()

        self.transformer = TransformationLayer(method='standard')

        self.validator = FeatureValidator()

    def run(self, horizon_hours=1):
        print("\n🚀 Starting Pipeline...\n")

        # 1. Ingestion
        raw_data = self.ingestor.ingest(horizon_hours)
        print("Ingested:", raw_data.shape)

        # 2. Preprocessing
        clean_data = self.preprocessor.clean(raw_data)
        print("Preprocessed:", clean_data.shape)

        # 3. Horizon
        filtered = self.window_engine.apply_horizon(clean_data, horizon_hours = 6)

        # 4. Windowing
        engine = WindowingEngine(
        window_size="30min",
        stride="5min",
        type="sliding"
        )
        windows = engine.create_windows(filtered)
        print("Windows:", len(windows))

        if not windows:
            raise ValueError("No windows created")

        # 5. Aggregation
        features = self.agg_engine.aggregate(windows)
        print("Features:", features.shape)

        # 6. Transformation
        self.transformer.fit(features)
        scaled = self.transformer.transform(features)

        # 7. Validation
        if not self.validator.validate(scaled):
            raise ValueError("Validation failed")

        print("\n✅ Pipeline Completed!")

        # 8. Save to CSV
        scaled.to_csv("/app/features_output.csv")
        print("✅ Features saved to features_output.csv")

        print("\n✅ Pipeline Completed!")

        return scaled

