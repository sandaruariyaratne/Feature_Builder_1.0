from pipeline import FeatureBuilderPipeline

if __name__ == "__main__":
    pipeline = FeatureBuilderPipeline()

    features = pipeline.run(horizon_hours=6)

    print(features.head(20))