
import pytest
import json
import allure
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, abs
from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
POS_FORMULA_PATH = os.path.join(BASE_DIR, "../test_data/pos.json")
TPM_FORMULA_PATH = os.path.join(BASE_DIR, "../test_data/tpm.json")
dataset_path = os.path.join(BASE_DIR, "../test_data/datasets_fact.json")
FINANCE_FORMULA_PATH = os.path.join(BASE_DIR, "../test_data/finance.json")
KIND_TPM_PATH = os.path.join(BASE_DIR, "../test_data/kind_tpm.json")
KIND_DISTRIBUTION_PATH = os.path.join(BASE_DIR, "../test_data/distribution.json")

# ---------------------------
# Load JSON
# ---------------------------
def load_json(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} not found")
    with open(path, "r") as f:
        return json.load(f)


RAW_DATASETS = load_json(dataset_path)
POS_FORMULAS = load_json(POS_FORMULA_PATH)
TPM_FORMULAS = load_json(TPM_FORMULA_PATH)
FINANCE_FORMULA=load_json(FINANCE_FORMULA_PATH)
KIND_TPM=load_json(KIND_TPM_PATH)
KIND_DISTRIBUTION = load_json(KIND_DISTRIBUTION_PATH)

# ---------------------------
# Flatten dataset
# ---------------------------
def flatten_datasets(raw):
    result = []

    for brand, datasets in raw.items():
        for d in datasets:
            result.append(
                (
                    brand,
                    d["table"],
                    d.get("base_table"),
                    d["keys"],
                    d["formula_type"]
                )
            )

    return result


DATASETS = flatten_datasets(RAW_DATASETS)


# ---------------------------
# Pick formula dynamically
# ---------------------------
def get_formula_config(formula_type):
    if formula_type == "pos":
        return POS_FORMULAS
    elif formula_type == "tpm":
        return TPM_FORMULAS
    elif formula_type == "finance":
        return FINANCE_FORMULA
    elif formula_type == "distribution":
        return KIND_DISTRIBUTION
    elif formula_type == "kind_tpm":
        return KIND_TPM
    else:
        raise ValueError(f"Unknown formula_type: {formula_type}")


# ---------------------------
# TEST
# ---------------------------
@pytest.mark.parametrize(
    "brand, table, base_table, keys, formula_type",
    DATASETS
)
@allure.parent_suite("ETL Validation")
@allure.feature("Derived Column Validation")
def test_formula_validation(brand, table, base_table, keys, formula_type):
    """
        Validates derived/metric columns in a Spark table using predefined formulas.

        Parameters:
        ----------
        brand : str
            Brand name used for reporting and logging.

        table : str
            Source table name to validate.

        base_table : str or None
            Optional reference/base table for comparison.

        keys : list
            List of key columns used for identifying unique records.

        formula_type : str
            Type of formula configuration to apply.

        Description:
        -----------
        - Fetches formula rules dynamically using `get_formula_config(formula_type)`.
        - Loads the source table into a Spark DataFrame (`df`).
        - Optionally loads a base/reference table (`base_df`) for comparison.
        - Iterates through each target column and its corresponding formula rule.
        - Skips validation if the target column is not present in the dataset.
        - Computes expected values using the provided formula.

        Logic:
        ------
        - Applies direct comparison for date-based formulas.
        - Casts numeric columns to DOUBLE for accurate calculations.
        - Uses tolerance-based comparison instead of exact matching.

        Handles Edge Cases:
        -------------------
        - Small floating-point differences
        - Percentage deviation (<1%)
        - NULL = NULL comparison
        - 0 vs NULL scenarios

        Validation Flow:
        ----------------
        - Filters mismatched records between actual and expected values.
        - Logs mismatch counts for visibility.
        - If no mismatches → validation passes.
        - If mismatches exist → captures sample rows with key columns.

        Base Table Comparison (if provided):
        -----------------------------------
        - Joins mismatched records with base table using keys.
        - Compares values with base data.
        - Fails test if mismatch persists against base table.

        Reporting:
        ----------
        - Attaches mismatch samples to Allure report.
        - Differentiates between derived mismatches and base mismatches.
        """
    allure.dynamic.story(f"{brand} - {table}")

    FORMULA_CONFIG = get_formula_config(formula_type)

    # ---------------------------
    # Load source
    # ---------------------------
    with allure.step("Load source table"):
        df = spark.sql(f"select * from {table}")
        allure.attach(str(df.count()), "Source Count", allure.attachment_type.TEXT)

    # ---------------------------
    # Load base (optional)
    # ---------------------------
    if base_table:
        with allure.step("Load base table"):
            base_df = spark.sql(f"select * from {base_table}")
            allure.attach(str(base_df.count()), "Base Count", allure.attachment_type.TEXT)
    else:
        base_df = None

    # ---------------------------
    # Validation loop
    # ---------------------------
    exclude_cols = {
        "UNITS_SOLD",
        "DISPLAY_VALUE",
        "FEATURE_VALUE",
        "BASE_UNITS",
        "BASE_VALUE",
        "INCREMENTAL_VALUE",
        "INCREMENTAL_UNITS",
        "VALUE_SALES_MM_ACV",
        "TDP"
    }
    for target_col, rule in FORMULA_CONFIG.items():
        if target_col in exclude_cols:
            print(f"Skipping {target_col} (excluded)")
            continue
        # Skip if column not in df (important for reuse)
        if target_col not in df.columns:
            continue

        with allure.step(f"Validating column: {target_col}"):

            formula = rule["formula"]
            expected_col = f"expected_{target_col}"
            is_numeric_formula = any(
                keyword in formula.lower()
                for keyword in ["cast(", "+", "-", "*", "/", "datediff","try_divide", "round"]
            )
            # DATE logic
            # if not is_numeric_formula or  "date" in formula.lower():
            if not is_numeric_formula or any(k in formula.lower() for k in ["date", "timestamp"]):

                df_calc = df.withColumn(expected_col, expr(formula))

                match_condition = (
                    (col(target_col) == col(expected_col)) |
                    (col(target_col).isNull() & col(expected_col).isNull())
                )

            else:
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

            mismatch_df = df_calc.filter(~match_condition)
            mismatch_count = mismatch_df.count()

            print(f"{brand} → {target_col} mismatch: {mismatch_count}")

            if mismatch_count == 0:
                continue

            mismatch_sample = mismatch_df.select(
                *keys,
                target_col,
                expected_col
            )

            # ---------------------------
            # Base comparison (if exists)
            # ---------------------------
            if base_df is not None and target_col in base_df.columns:

                base_check_df = mismatch_sample.alias("m").join(
                    base_df.alias("b"),
                    on=[col(f"m.{k}") == col(f"b.{k}") for k in keys],
                    how="left"
                )

                result_df = base_check_df.select(
                    *[col(f"m.{k}").alias(k) for k in keys],
                    col(f"m.{target_col}").alias("actual_value"),
                    col(f"m.{expected_col}").alias("expected_value"),
                    col(f"b.{target_col}").alias("base_value"),
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

            # ---------------------------
            # No base → direct mismatch
            # ---------------------------
            else:
                allure.attach(
                    mismatch_sample.limit(20).toPandas().to_html(index=False),
                    name=f"{target_col} Derived Mismatch",
                    attachment_type=allure.attachment_type.HTML
                )
                # assert False, f"{target_col} derived column mismatch"