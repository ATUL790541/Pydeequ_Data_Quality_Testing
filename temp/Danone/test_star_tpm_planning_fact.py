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

from etl_test.utils.testdata_reader import load_test_cases

# ---------------------------
# Fixture
# ---------------------------
@pytest.fixture(scope="module")
def input_data():

    spark = SparkSession.getActiveSession()

    df = spark.sql(
      "select * from asper_production_danone_us_production_catalog.silver_us.star_tpm_planning_fact"
    )

    binary_df = spark.read.format("binaryFile") \
        .load("/Volumes/asper_production_danone_us_production_catalog/silver_us/silver_volume/ADP Silver Schema-Latest.xlsx")

    file_bytes = binary_df.select("content").collect()[0][0]

    excel_file = pd.ExcelFile(io.BytesIO(file_bytes))

    base_df = pd.read_excel(
        excel_file,
        sheet_name="TPM Planning Fact",
        header=None
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
    # load_test_cases("tpm_planning_fact.py")
    df, base_df = input_data
    #
    # with allure.step("Run schema validation"):
    #     result = full_schema_validation(df, base_df)
    #
    # with allure.step("Attach validation results"):
    #     allure.attach(
    #         str(result),
    #         name="Schema Validation Result",
    #         attachment_type=allure.attachment_type.TEXT
    #     )
    #
    # assert result["status"], f"""
    # Unexpected: {result['unexpected_columns']}
    # Missing: {result['missing_columns']}
    # """



    expected_cols = load_test_cases("tpm_planning_fact.py", "EXPECTED_COLUMNS")

    actual_cols = [c.strip().lower() for c in df.columns]
    expected_cols = [c.strip().lower() for c in expected_cols]

    missing = [col for col in expected_cols if col not in actual_cols]

    # Attach
    allure.attach(
        pd.DataFrame(missing, columns=["Missing Columns"]).to_html(index=False),
        name="Missing Columns",
        attachment_type=allure.attachment_type.HTML
    )

    assert len(missing) == 0, f"Missing columns: {missing}"


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
"PAR_NUM",
    "CONTRACT_NUM",
    "FUND_REQUEST",
    "COMPENSATION_TYPE",
    "SALES_ADJUSTED_BASE",
    "SMOOTH_BASE_VOLUME",
    "UPLIFT_VOLUME",
    "OVERLAPPING_LIFT",
    "PLANNED_SHIPMENT_VOLUME",
    "TOTAL_CONSUMPTION_VOLUME",
    "PURCHASE_QUANTITY",
    "USER_INPUT_QUANTITY",
    "REGULAR_PRICE",
    "MY_PROMOTED_PRICE",
    "MATERIAL_LIST_PRICE",
    "LIST_PRICE_PER_UNIT",
    "BASE_PRICE_PER_UNIT",
    "NET_UNIT_PRICE",
    "FINAL_LIST_PRICE",
    "SAVINGS_AMOUNT",
    "OFF_INVOICE_PERCENT",
    "OI_RATE",
    "NBOI_RATE",
    "BILL_BACK_RATE",
    "MY_RATE",
    "PROMO_RATE",
    "PROMO_RATE_UOM",
    "SCAN_FLAG",
    "SCAN_REDEMPTION_PERCENT",
    "SCAN_VOLUME",
    "REVISED_SCAN_VOLUME",
    "SYNDICATED_SCAN_VOLUME",
    "FIXED_AD_FEES",
    "FIXED_DISPLAY_FEES",
    "FIXED_MARKDOWN_FEES",
    "FIXED_MISC",
    "SLOTTING_FIXED",
    "FIXED_COUPON_CLEARING_HOUSE",
    "FIXED_NON_COUPON_CLEARING_HOUSE",
    "FIXED_PROGRAM_COST",
    "TACTIC_SPEND_LATEST_ESTIMATE",
    "TACTIC_RATE_LUMPSUM",
    "SPEND_ALLOCATED",
    "FULL_DISPLAY_ACV_PERC",
    "FULL_FEATURE_ACV_PERC",
    "ACV_PERC",
    "SALE_DURATION_DAYS",
    "PRODUCT_PLANNING_LEVEL",
    "BELOW_LUC_INDICATOR",
    "BELOW_MPIC_INDICATOR",
    "POST_AUDIT_AMOUNT",
    "WRITE_OFF_AMOUNT"
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


    keys = ["SALES_ORGANIZATION_CODE", "PROMOTION_ID", "EVENT_ID","MATERIAL_GROUP_ID"]

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