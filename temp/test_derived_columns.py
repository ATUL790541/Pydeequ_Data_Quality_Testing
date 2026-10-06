import pytest
import json
import allure
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, abs

import os
spark = SparkSession.getActiveSession()

# ---------------------------
# Resolve paths (IMPORTANT)
# ---------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
dataset_path = os.path.join(BASE_DIR, "../test_data/datasets_fact.json")
json_path = os.path.join(BASE_DIR, "formula.json")


# ---------------------------
# Load JSON
# ---------------------------
def load_json(path):
    with open(path, "r") as f:
        return json.load(f)


RAW_DATASETS = load_json(dataset_path)
FORMULA_CONFIG = load_json(json_path)


# ---------------------------
# Flatten dataset config
# ---------------------------
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

# ---------------------------
# TEST
# ---------------------------
@pytest.mark.parametrize(
    "brand, table, base_table, keys",
    DATASETS
)
@allure.parent_suite("ETL Validation")
@allure.feature("Derived Column Validation")
def test_formula_validation(brand, table, base_table, keys):

    allure.dynamic.story(f"{brand} - {table}")

    # ---------------------------
    # Load data
    # ---------------------------
    with allure.step("Load source and base tables"):
        df = spark.sql(f"select * from {table}")
        base_df = spark.sql(f"select * from {base_table}")

        allure.attach(str(df.count()), "Source Count", allure.attachment_type.TEXT)
        allure.attach(str(base_df.count()), "Base Count", allure.attachment_type.TEXT)

    # ---------------------------
    # Validation loop (YOUR LOGIC)
    # ---------------------------
    for target_col, rule in FORMULA_CONFIG.items():

        with allure.step(f"Validating column: {target_col}"):

            formula = rule["formula"]
            expected_col = f"expected_{target_col}"

            # DATE logic
            if "date" in formula.lower():

                df_calc = df.withColumn(expected_col, expr(formula))

                match_condition = (
                    (col(target_col) == col(expected_col)) |
                    (col(target_col).isNull() & col(expected_col).isNull())
                )

            else:
                # Numeric logic
                df_calc = df.withColumn(
                    expected_col,
                    expr(f"CAST(({formula}) AS DOUBLE)")
                )

                tolerance = rule.get("tolerance", 0.001)

                match_condition = (
                    (abs(col(target_col).cast("double") - col(expected_col).cast("double")) < tolerance)
                    | (
                        abs(col(target_col).cast("double") - col(expected_col).cast("double")) /
                        (abs(col(expected_col).cast("double")) + 1e-9) < 0.01
                    )
                    | (col(target_col).isNull() & col(expected_col).isNull())
                    | ((col(target_col) == 0) & col(expected_col).isNull())
                    | (
                        (col(target_col) == 0) &
                        (abs(col(expected_col) - 1) < 0.0001)
                    )
                )

            # ---------------------------
            # Mismatch
            # ---------------------------
            mismatch_df = df_calc.filter(~match_condition)
            mismatch_count = mismatch_df.count()

            print(f"{brand} → {target_col} mismatch: {mismatch_count}")

            # allure.attach(
            #     str(mismatch_count),
            #     name=f"{target_col} mismatch count",
            #     attachment_type=allure.attachment_type.TEXT
            # )

            if mismatch_count == 0:
                continue

            mismatch_sample = mismatch_df.select(
                *keys,
                target_col,
                expected_col
            )

            base_has_column = target_col in base_df.columns

            base_check_df = mismatch_sample.alias("m").join(
                base_df.alias("b"),
                on=[col(f"m.{k}") == col(f"b.{k}") for k in keys],
                how="left"
            )

            if base_has_column:

                result_df = base_check_df.select(
                    *[col(f"m.{k}").alias(k) for k in keys],
                    col(f"m.{target_col}").alias("actual_value"),
                    col(f"m.{expected_col}").alias("expected_value"),
                    col(f"b.{target_col}").alias("base_value"),
                    col(f"b.{keys[0]}").isNotNull().alias("row_present_in_base"),
                    (col(f"m.{target_col}") == col(f"b.{target_col}")).alias("value_match_base")
                )

                mismatch_base_df = result_df.filter(col("value_match_base") == False)

                if mismatch_base_df.limit(1).count() > 0:

                    allure.attach(
                        mismatch_base_df.limit(20).toPandas().to_html(index=False),
                        name=f"{target_col} Base Mismatch",
                        attachment_type=allure.attachment_type.HTML
                    )

                    assert False, f"{target_col} mismatch with base table"

            else:
                allure.attach(
                    mismatch_sample.limit(10).toPandas().to_html(index=False),
                    name=f"{target_col} Derived Mismatch",
                    attachment_type=allure.attachment_type.HTML
                )

                assert False, f"{target_col} derived column mismatch"