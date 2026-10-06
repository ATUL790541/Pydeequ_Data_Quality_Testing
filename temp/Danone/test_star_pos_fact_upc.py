import pytest
import pandas as pd
import io
import allure
from pyspark.sql import SparkSession
from utils.validations import (
    full_schema_validation,
    get_null_counts,
    get_min_max,
    check_composite_key_duplicates
)


# ---------------------------
# Fixture
# ---------------------------
@pytest.fixture(scope="module")
def input_data():

    spark = SparkSession.getActiveSession()

    df = spark.read.csv(
        "/Volumes/asper_product_databricks_catlog/test/danone_pod_fact/star_pos_fact_upc-2.csv",
        header=True,
        inferSchema=True
    )

    binary_df = spark.read.format("binaryFile") \
        .load("/Volumes/asper_production_danone_us_production_catalog/silver_us/silver_volume/ADP Silver Schema-Latest.xlsx")

    file_bytes = binary_df.select("content").collect()[0][0]

    excel_file = pd.ExcelFile(io.BytesIO(file_bytes))

    base_df = pd.read_excel(
        excel_file,
        sheet_name="Pos Fact (UPC)",
        header=1
    )

    return df, base_df

@allure.story("Record Count Check")
def test_record_count(input_data):

    df, _ = input_data

    with allure.step("Calculate total record count"):
        total_count = df.count()

    # Attach count to Allure
    allure.attach(
        str(total_count),
        name="Total Record Count",
        attachment_type=allure.attachment_type.TEXT
    )

    print(f"Total records: {total_count}")

    # Basic validation (optional)
    assert total_count > 0, "Dataset is empty"

# ---------------------------
# Schema Validation
# ---------------------------
@allure.feature("ETL Validation")
@allure.story("Schema Validation")
def test_dataset_schema_conforms_to_adp_silver_schema(input_data):
    df, base_df = input_data

    with allure.step("Run schema validation"):
        result = full_schema_validation(df, base_df)

    with allure.step("Attach validation results"):
        allure.attach(
            str(result),
            name="Schema Validation Result",
            attachment_type=allure.attachment_type.TEXT
        )

    assert result["status"], f"""
    Unexpected: {result['unexpected_columns']}
    Missing: {result['missing_columns']}
    """


# ---------------------------
# 🔹 Null Validation
# ---------------------------
@allure.feature("ETL Validation")

@allure.story("Null Check")
def test_nulls(input_data):

    df, _ = input_data

    with allure.step("Calculate null counts"):
        null_counts = get_null_counts(df)

    # Convert to DataFrame
    null_df = pd.DataFrame(
        list(null_counts.items()),
        columns=["Column Name", "Null Count"]
    )

    # Filter failed columns
    failed_df = null_df[null_df["Null Count"] > 0]

    # Attach full table
    allure.attach(
        null_df.to_csv(index=False),
        name="Null Counts (All Columns)",
        attachment_type=allure.attachment_type.CSV
    )

    # Attach only failed columns (important)
    if not failed_df.empty:
        allure.attach(
            failed_df.to_csv(index=False),
            name="Columns with Nulls",
            attachment_type=allure.attachment_type.CSV
        )

    assert failed_df.empty, f"Null values found:\n{failed_df}"


# ---------------------------
# 🔹 Min/Max Validation
# ---------------------------
columns = [
    "ACV_DISPLAY_ONLY",
    "ACV_FEATURE_ONLY",
    "ACV_ANY_PROMO",
    "WEEKLY_ACV",
    "VALUE_SALES",
    "DISPLAY_VALUE",
    "FEATURE_VALUE",
    "SALES_WITH_ANY_PROMO",
    "TPR_VALUE",
    "SALES_WITH_NO_PROMO",
    "UNIT_PRICE",
    "PROMO_UNIT_PRICE",
    "BASE_UNIT_PRICE",
    "UNIT_SALES",
    "DISPLAY_UNITS",
    "FEATURE_UNITS",
    "UNITS_WITH_ANY_PROMO",
    "TPR_UNITS",
    "UNITS_WITH_NO_PROMO",
    "EQ_VOLUME_SALES"
]


@allure.feature("ETL Validation")
@allure.story("Min-Max Validation")
def test_min_max(input_data):

    df, _ = input_data

    with allure.step("Calculate min/max values"):
        result = get_min_max(df, columns)

    # Convert to readable format
    formatted = "\n".join(
        [f"{col} -> Min: {mn}, Max: {mx}" for col, mn, mx in result]
    )

    allure.attach(
        formatted,
        name="Min-Max Results",
        attachment_type=allure.attachment_type.TEXT
    )

    assert len(result) > 0




@allure.story("Duplicate Check")
def test_composite_key_duplicates(input_data):

    df, _ = input_data

    keys = ["UPC", "GEOGRAPHY_DESCRIPTION", "WEEK_ENDING_DATE"]

    with allure.step(f"Checking duplicates for keys: {keys}"):
        dup_df, dup_count = check_composite_key_duplicates(df, keys)

    # Attach total count
    allure.attach(
        str(dup_count),
        name="Total Duplicate Key Count",
        attachment_type=allure.attachment_type.TEXT
    )

    # Convert limited rows to pandas
    dup_pd = dup_df.limit(50).toPandas()

    if not dup_pd.empty:
        allure.attach(
            dup_pd.to_csv(index=False),
            name="Duplicate Records (CSV)",
            attachment_type=allure.attachment_type.CSV
        )

        allure.attach(
            dup_pd.to_html(index=False),
            name="Duplicate Records Table",
            attachment_type=allure.attachment_type.HTML
        )

    assert dup_count == 0, f"Duplicate records found: {dup_count}"