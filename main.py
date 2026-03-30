from pipeline import FeatureBuilderPipeline
import json

pipeline = FeatureBuilderPipeline()

payload = {
    "application_id": 1,
    "blackbox_probe_name": "blackbox-app",
    "container_name": "backend",
    "lookback_window": "5m",
    "selection_mode": "manual",
    "manual_start": "2026-03-26T10:00:00",
    "manual_end": "2026-03-26T11:00:00"
}

features = pipeline.run(payload)

# Save to JSON file
with open("features_output.json", "w") as f:
    json.dump(features, f, indent=2)

print("✅ JSON saved to features_output.json")