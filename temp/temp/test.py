
import os
import json
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
dataset_path = os.path.join(BASE_DIR, "../test_data/fact_datasets.json")
json_path = os.path.join(BASE_DIR, "../test_data/pos.json")

# ---------------------------
# Load JSON
# ---------------------------
def load_json(path):

    with open(path, "r") as f:
        return json.load(f)


RAW_DATASETS = load_json(dataset_path)
# print(RAW_DATASETS)
FORMULA_CONFIG = load_json(json_path)
# print(FORMULA_CONFIG)

def flatten_datasets(raw):
    result = []

    for brand, datasets in raw.items():
        for d in datasets:
            result.append(
                (
                    brand,
                    d["table"],
                    d["base_table"],
                    d["keys"]
                )
            )

    return result


DATASETS = flatten_datasets(RAW_DATASETS)

print(DATASETS)