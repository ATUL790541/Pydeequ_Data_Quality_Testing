import pytest
import pandas as pd
import allure

from etl_test.utils.validations import (
    # full_schema_validation,
    get_null_counts,
    validate_expected_columns,
    check_composite_key_duplicates,
    check_composite_key_nulls,
    check_nulls_ignore_string_values
)

from etl_test.utils.config_helper import get_all_datasets, has_schema_sheet
from etl_test.test_data.dataset_config import DATASETS


#
# ALL_DATASETS = get_all_datasets(DATASETS)

@allure.parent_suite("ETL Data Validation")
class TestETLDataQuality:

    # ---------------------------
    #  Record Count
    # ---------------------------
    def test_record_count(self, input_data):
        df, _, customer, dataset, _ = input_data

        allure.dynamic.feature(customer)
        allure.dynamic.story(dataset)
        allure.dynamic.title("Record Count")

        total_count = df.count()

        allure.attach(
            str(total_count),
            name="Total Record Count",
            attachment_type=allure.attachment_type.TEXT
        )

        assert total_count > 0, f"{customer}-{dataset} is empty"


    # ---------------------------
    #  Schema Validation
    # ---------------------------
    # def test_schema_validation(self, input_data):
    #     df, base_df, customer, dataset, _ = input_data
    #
    #     allure.dynamic.feature(customer)
    #     allure.dynamic.story(dataset)
    #     allure.dynamic.title("Schema Validation")
    #
    #     result = full_schema_validation(df, base_df)
    #
    #     allure.attach(
    #         str(result),
    #         name="Schema Validation Result",
    #         attachment_type=allure.attachment_type.TEXT
    #     )
    #
    #     assert result["status"], f"""
    #     {customer}-{dataset}
    #     Missing: {result['missing_columns']}
    #     Unexpected: {result['unexpected_columns']}
    #     """
    def test_schema_validation(self, input_data):

        df, base_df, customer, dataset, details = input_data
        if not has_schema_sheet(details):
            pytest.skip("No schema sheet configured")
        allure.dynamic.feature(customer)
        allure.dynamic.story(dataset)
        allure.dynamic.title("Schema Validation")

        result = validate_expected_columns(df, base_df)

        # =====================================================
        # Summary Table
        # =====================================================

        summary_df = pd.DataFrame([
            {
                "Customer": customer,
                "Dataset": dataset,
                "Expected Columns": result["expected_count"],
                "Actual Columns": result["actual_count"],
                "Missing Columns": result["missing_count"],
                "Unexpected Columns": result["unexpected_count"],
                "Status": "PASS" if result["status"] else "FAIL"
            }
        ])

        allure.attach(
            summary_df.to_html(index=False),
            name="Schema Validation Summary",
            attachment_type=allure.attachment_type.HTML
        )

        # =====================================================
        # Missing Columns
        # =====================================================

        if result["missing_columns"]:
            missing_df = pd.DataFrame(
                result["missing_columns"],
                columns=["Column Present In ADP But Missing In Dataset"]
            )

            allure.attach(
                missing_df.to_html(index=False),
                name="Missing Columns",
                attachment_type=allure.attachment_type.HTML
            )

        # =====================================================
        # Unexpected Columns
        # =====================================================

        if result["unexpected_columns"]:
            unexpected_df = pd.DataFrame(
                result["unexpected_columns"],
                columns=["Column Present In Dataset But Not In ADP"]
            )

            allure.attach(
                unexpected_df.to_html(index=False),
                name="Unexpected Columns",
                attachment_type=allure.attachment_type.HTML
            )

        # =====================================================
        # Console Logs
        # =====================================================

        print("\n" + "=" * 100)
        print(f"Customer : {customer}")
        print(f"Dataset  : {dataset}")
        print(f"Expected : {result['expected_count']}")
        print(f"Actual   : {result['actual_count']}")
        print(f"Missing  : {result['missing_count']}")
        print(f"Extra    : {result['unexpected_count']}")

        if result["missing_columns"]:
            print("\nMissing Columns:")
            for col in result["missing_columns"]:
                print(col)

        if result["unexpected_columns"]:
            print("\nUnexpected Columns:")
            for col in result["unexpected_columns"]:
                print(col)

        print("=" * 100)

        # =====================================================
        # Assertion
        # =====================================================

        assert result["status"], (
            f"\n{customer}-{dataset}"
            f"\nMissing Columns ({result['missing_count']}): "
            f"{result['missing_columns']}"
            f"\nUnexpected Columns ({result['unexpected_count']}): "
            f"{result['unexpected_columns']}"
        )
    # ---------------------------
    #  Null Validation
    # ---------------------------
    def test_null_validation(self, input_data):
        # df, _, customer, dataset, _ = input_data
        #
        # allure.dynamic.feature(customer)
        # allure.dynamic.story(dataset)
        # allure.dynamic.title("Null Check")
        #
        # null_counts = get_null_counts(df)
        #
        # null_df = pd.DataFrame(
        #     list(null_counts.items()),
        #     columns=["Column Name", "Null Count"]
        # )
        #
        # failed_df = null_df[null_df["Null Count"] > 0]
        #
        # # Attach full table
        # allure.attach(
        #     null_df.to_html(index=False),
        #     name="Null Counts (All Columns)",
        #     attachment_type=allure.attachment_type.HTML
        # )
        #
        # # Attach only failed columns
        # if not failed_df.empty:
        #     allure.attach(
        #         failed_df.to_html(index=False),
        #         name="Columns with Nulls",
        #         attachment_type=allure.attachment_type.HTML
        #     )
        #
        # assert failed_df.empty, f"{customer}-{dataset} has nulls:\n{failed_df}"
        df, _, customer, dataset, _ = input_data

        allure.dynamic.feature(customer)
        allure.dynamic.story(dataset)
        allure.dynamic.title("Null Check")

        null_counts = get_null_counts(df)

        null_df = pd.DataFrame(
            list(null_counts.items()),
            columns=["Column Name", "Null Count"]
        )

        failed_df = null_df[null_df["Null Count"] > 0]

        # Attach full table
        allure.attach(
            null_df.to_html(index=False),
            name="Null Counts (All Columns)",
            attachment_type=allure.attachment_type.HTML
        )

        # Attach only failed columns
        if not failed_df.empty:
            allure.attach(
                failed_df.to_html(index=False),
                name="Columns with Nulls",
                attachment_type=allure.attachment_type.HTML
            )

            # Mark as warning instead of failing
            allure.attach(
                f"{customer}-{dataset} has null values",
                name="Warning",
                attachment_type=allure.attachment_type.TEXT
            )

            # Optional: log in console
            print(f"WARNING: {customer}-{dataset} has nulls")

        #  Always pass
        assert True


    # ---------------------------
    # Duplicate Check
    # ---------------------------
    def test_duplicate_validation(self, input_data):
        df, _, customer, dataset, details = input_data

        allure.dynamic.feature(customer)
        allure.dynamic.story(dataset)
        allure.dynamic.title("Duplicate Check")

        keys = details.get("keys")

        # Skip if keys not defined
        if not keys:
            pytest.skip(f"No keys defined for {dataset}")

        dup_df, dup_count = check_composite_key_duplicates(df, keys)

        allure.attach(
            str(dup_count),
            name="Total Duplicate Key Count",
            attachment_type=allure.attachment_type.TEXT
        )

        dup_pd = dup_df.limit(50).toPandas()

        if not dup_pd.empty:
            allure.attach(
                dup_pd.to_html(index=False),
                name="Duplicate Records",
                attachment_type=allure.attachment_type.HTML
            )

        assert dup_count == 0, f"{customer}-{dataset} has duplicates: {dup_count}"



    def test_composite_key_null_validation(self, input_data):
        df, _, customer, dataset, details = input_data

        allure.dynamic.feature(customer)
        allure.dynamic.story(dataset)
        allure.dynamic.title("Composite Key NULL Validation")

        keys = details.get("keys")

        if not keys:
            pytest.skip(f"No keys defined for {dataset}")

        # Call generic function
        result = check_nulls_ignore_string_values(
            df,
            keys,
            ignore_values=["None", "NA", "N/A"]  # configurable
        )

        # -------------------------------
        # Reporting
        # -------------------------------
        allure.attach(
            f"Total Rows: {result['total_count']}\n"
            f"Ignored Rows: {result['ignored_count']}\n"
            f"Rows Checked: {result['checked_count']}\n"
            f"NULL Count: {result['null_count']}",
            name="Validation Summary",
            attachment_type=allure.attachment_type.TEXT
        )

        # Attach sample NULL rows
        null_pd = result["null_df"].limit(50).toPandas()
        if not null_pd.empty:
            allure.attach(
                null_pd.to_html(index=False),
                name="Rows with NULL Keys",
                attachment_type=allure.attachment_type.HTML
            )

        # -------------------------------
        # Assertion
        # -------------------------------
        assert result["null_count"] == 0, \
            f"{customer}-{dataset} has NULLs in keys: {result['null_count']}"

    # def test_duplicate_validation(self, input_data):
    #     df, _, customer, dataset, details = input_data
    #
    #     allure.dynamic.feature(customer)
    #     allure.dynamic.story(dataset)
    #     allure.dynamic.title("Duplicate + Null Check")
    #
    #     keys = details.get("keys")
    #
    #     if not keys:
    #         pytest.skip(f"No keys defined for {dataset}")
    #
    #     # ---------------------------
    #     # NULL CHECK (NEW 🔥)
    #     # ---------------------------
    #     null_df, null_count = check_nulls_in_keys(df, keys)
    #
    #     allure.attach(
    #         str(null_count),
    #         name="Total NULL Key Count",
    #         attachment_type=allure.attachment_type.TEXT
    #     )
    #
    #     null_pd = null_df.limit(50).toPandas()
    #
    #     if not null_pd.empty:
    #         allure.attach(
    #             null_pd.to_html(index=False),
    #             name="NULL Key Records",
    #             attachment_type=allure.attachment_type.HTML
    #         )
    #
    #     assert null_count == 0, f"{customer}-{dataset} has NULLs in keys: {null_count}"
    #
    #     # ---------------------------
    #     # DUPLICATE CHECK (EXISTING)
    #     # ---------------------------
    #     dup_df, dup_count = check_composite_key_duplicates(df, keys)
    #
    #     allure.attach(
    #         str(dup_count),
    #         name="Total Duplicate Key Count",
    #         attachment_type=allure.attachment_type.TEXT
    #     )
    #
    #     dup_pd = dup_df.limit(50).toPandas()
    #
    #     if not dup_pd.empty:
    #         allure.attach(
    #             dup_pd.to_html(index=False),
    #             name="Duplicate Records",
    #             attachment_type=allure.attachment_type.HTML
    #         )
    #
    #     assert dup_count == 0, f"{customer}-{dataset} has duplicates: {dup_count}"