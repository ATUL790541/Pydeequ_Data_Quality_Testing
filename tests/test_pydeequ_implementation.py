"""
PyDeequ Data Quality Implementation - Production Ready
======================================================
Complete implementation of PyDeequ for data quality analysis and verification.
Tested on Databricks with Spark 3.5.0 | PyDeequ 1.3.0

Includes:
- Data Analyzers (Completeness, Uniqueness, Compliance, etc.)
- Constraint Checks (Verification)
- Quality Metrics Computation
- Comprehensive Reporting

Usage:
    pytest test_pydeequ_implementation.py --brand=Danone --dataset=all
"""

import pytest
import pandas as pd
import allure
from typing import List, Optional, Dict, Any, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, count, when, isnan, isnull, lower, trim

from etl_test.utils.validations import validate_expected_columns
from etl_test.utils.config_helper import has_schema_sheet

from pydeequ.analyzers import (
    AnalysisRunner,
    AnalyzerContext,
    Completeness,
    Uniqueness,
    Distinctness,
    CountDistinct,
    Size,
    Mean,
    StandardDeviation,
    Minimum,
    Maximum,
    Sum,
    Compliance,
    PatternMatch,
    Entropy,
)
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult


# ==================================================================================
# SECTION 1: UTILITY FUNCTIONS FOR PYDEEQU ANALYZERS
# ==================================================================================

def run_analyzers(spark: SparkSession, df: DataFrame, analyzers: List[Any]) -> DataFrame:
    """
    Run a list of pydeequ analyzer objects against a DataFrame.

    Args:
        spark: SparkSession instance
        df: Input Spark DataFrame
        analyzers: List of pydeequ analyzer instances

    Returns:
        Spark DataFrame with columns: entity, instance, name, value
    """
    if not analyzers:
        raise ValueError("`analyzers` must contain at least one analyzer instance.")

    analysis_runner = AnalysisRunner(spark).onData(df)

    for analyzer in analyzers:
        analysis_runner = analysis_runner.addAnalyzer(analyzer)

    analysis_result = analysis_runner.run()
    return AnalyzerContext.successMetricsAsDataFrame(spark, analysis_result)


def _metric(metrics_pd: pd.DataFrame, name: str) -> float:
    """Pull a single analyzer value out of a PyDeequ successMetrics frame."""
    rows = metrics_pd[metrics_pd["name"] == name]
    if rows.empty:
        raise KeyError(f"PyDeequ metric {name!r} not present in analyzer results")
    return float(rows.iloc[0]["value"])


def _ratio_to_count(total: int, ratio: float) -> int:
    """
    Convert a PyDeequ ratio metric back into an exact row count.

    round() is mandatory here, not int(): Completeness 0.9 over 10 rows makes
    10 * (1 - 0.9) == 0.9999999999999998, which int() truncates to 0.
    """
    return int(round(total * ratio))


def get_null_counts_pydeequ(spark: SparkSession, df: DataFrame) -> Dict[str, int]:
    """
    Per-column null counts derived from the PyDeequ Completeness analyzer.

    Matches utils.validations.get_null_counts, which returns exact integers.
    """
    analyzers = [Size()] + [Completeness(c) for c in df.columns]
    metrics_pd = run_analyzers(spark, df, analyzers).toPandas()

    total_rows = int(_metric(metrics_pd, "Size"))

    completeness = metrics_pd[metrics_pd["name"] == "Completeness"]
    measured = {
        row["instance"]: _ratio_to_count(total_rows, 1.0 - float(row["value"]))
        for _, row in completeness.iterrows()
    }

    return {c: measured.get(c, 0) for c in df.columns}


def check_composite_key_duplicates_pydeequ(spark: SparkSession, df: DataFrame,
                                           keys: List[str]) -> Dict[str, Any]:
    """
    Duplicate composite-key GROUP count, derived purely from PyDeequ analyzers.

    Parity with utils.validations.check_composite_key_duplicates: only current
    records are considered when an `is_current` column exists, and the number
    reported is duplicated key GROUPS rather than duplicated rows.

    PyDeequ defines Uniqueness as (#key combos occurring exactly once) / Size,
    so duplicate_groups == CountDistinct(keys) - Uniqueness(keys) * Size().
    """
    is_current_col = next(
        (c for c in df.columns if c.lower() == "is_current"),
        None
    )
    scoped_df = df.filter(col(is_current_col) == True) if is_current_col else df

    analyzers = [Size(), CountDistinct(keys), Uniqueness(keys)]
    if len(keys) == 1:
        analyzers.append(Completeness(keys[0]))

    metrics_pd = run_analyzers(spark, scoped_df, analyzers).toPandas()

    size = int(_metric(metrics_pd, "Size"))
    distinct_keys = int(_metric(metrics_pd, "CountDistinct"))
    unique_once = _ratio_to_count(size, _metric(metrics_pd, "Uniqueness"))

    # Deequ's NULL handling differs between these two analyzers: Uniqueness
    # treats NULL as an ordinary value, while CountDistinct drops it for a
    # single column (though not for a multi-column key). Add the NULL group
    # back so the total matches groupBy, which is what the ETL suite counts.
    if len(keys) == 1 and _ratio_to_count(size, 1.0 - _metric(metrics_pd, "Completeness")):
        distinct_keys += 1

    return {
        "metrics": metrics_pd,
        "dup_count": distinct_keys - unique_once,
        "size": size,
        "distinct_keys": distinct_keys,
        "unique_once": unique_once,
        "is_current_col": is_current_col,
    }


def check_composite_key_nulls_pydeequ(spark: SparkSession, df: DataFrame, keys: List[str],
                                      ignore_values: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    NULL check over composite keys, measured with PyDeequ Size + Compliance.

    Parity with utils.validations.check_nulls_ignore_string_values, including
    its Step-1 behaviour: rows whose key matches an ignore value are filtered
    out before the NULL measurement is taken.
    """
    if not ignore_values:
        ignore_values = ["none"]

    ignore_values = [v.lower().strip() for v in ignore_values]

    filtered_df = df
    for k in keys:
        filtered_df = filtered_df.filter(~lower(trim(col(k))).isin(ignore_values))

    total_count = int(_metric(run_analyzers(spark, df, [Size()]).toPandas(), "Size"))

    predicate = " AND ".join(f"`{k}` IS NOT NULL" for k in keys)
    metrics_pd = run_analyzers(
        spark,
        filtered_df,
        [Size(), Compliance("composite_key_not_null", predicate)],
    ).toPandas()

    checked_count = int(_metric(metrics_pd, "Size"))

    # Compliance over an empty frame is undefined, and the ignore filters above
    # can legitimately remove every row.
    if checked_count:
        null_count = _ratio_to_count(checked_count, 1.0 - _metric(metrics_pd, "Compliance"))
    else:
        null_count = 0

    return {
        "metrics": metrics_pd,
        "total_count": total_count,
        "ignored_count": total_count - checked_count,
        "checked_count": checked_count,
        "null_count": null_count,
    }


def compute_completeness(spark: SparkSession, df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Compute completeness (fraction of non-null values) for given columns.

    Args:
        spark: SparkSession instance
        df: Input DataFrame
        columns: List of column names to analyze

    Returns:
        DataFrame with completeness metrics
    """
    if not columns:
        raise ValueError("`columns` must contain at least one column name.")

    analyzers = [Completeness(col) for col in columns]
    return run_analyzers(spark, df, analyzers)


def compute_uniqueness(spark: SparkSession, df: DataFrame, columns: List[str],
                      combined: bool = False) -> DataFrame:
    """
    Compute uniqueness for given column(s).

    Args:
        spark: SparkSession instance
        df: Input DataFrame
        columns: List of column names
        combined: If True, check combined uniqueness of all columns;
                 if False, check each column individually

    Returns:
        DataFrame with uniqueness metrics
    """
    if not columns:
        raise ValueError("`columns` must contain at least one column name.")

    if combined:
        analyzers = [Uniqueness(columns)]
    else:
        analyzers = [Uniqueness([col]) for col in columns]

    return run_analyzers(spark, df, analyzers)


def compute_distinctness(spark: SparkSession, df: DataFrame, columns: List[str],
                        individual: bool = True) -> DataFrame:
    """
    Compute distinctness (fraction of distinct values) for column(s).

    Args:
        spark: SparkSession instance
        df: Input DataFrame
        columns: List of column names
        individual: If True, compute per column; if False, combined

    Returns:
        DataFrame with distinctness metrics
    """
    if not columns:
        raise ValueError("`columns` must contain at least one column name.")

    if individual:
        analyzers = [Distinctness(col) for col in columns]
    else:
        analyzers = [Distinctness(columns), CountDistinct(columns)]

    return run_analyzers(spark, df, analyzers)


def compute_numeric_summary(spark: SparkSession, df: DataFrame,
                           columns: List[str]) -> DataFrame:
    """
    Compute mean, std dev, min, max, sum for numeric columns.

    Args:
        spark: SparkSession instance
        df: Input DataFrame
        columns: List of numeric column names

    Returns:
        DataFrame with numeric summary statistics
    """
    if not columns:
        raise ValueError("`columns` must contain at least one column name.")

    analyzers = [Size()]
    for col in columns:
        analyzers.extend([
            Mean(col),
            StandardDeviation(col),
            Minimum(col),
            Maximum(col),
            Sum(col),
        ])

    return run_analyzers(spark, df, analyzers)


def compute_standard_deviation(spark: SparkSession, df: DataFrame,
                               numeric_columns: List[str]) -> DataFrame:
    """
    Compute standard deviation for numeric columns.

    Args:
        spark: SparkSession instance
        df: Input DataFrame
        numeric_columns: List of numeric column names

    Returns:
        DataFrame with standard deviation metrics
    """
    analyzers = [StandardDeviation(col) for col in numeric_columns]
    return run_analyzers(spark, df, analyzers)


def compute_entropy(spark: SparkSession, df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Compute entropy (diversity measure) for columns.

    Args:
        spark: SparkSession instance
        df: Input DataFrame
        columns: List of column names

    Returns:
        DataFrame with entropy metrics
    """
    analyzers = [Entropy(col) for col in columns]
    return run_analyzers(spark, df, analyzers)


def compute_compliance(spark: SparkSession, df: DataFrame, column: str,
                      pattern: str, name: str = "compliance") -> DataFrame:
    """
    Check compliance of column values with a SQL pattern.

    Args:
        spark: SparkSession instance
        df: Input DataFrame
        column: Column name
        pattern: SQL pattern (e.g., "`email` RLIKE '.*@.*\\.com$'")
        name: Name for the compliance check

    Returns:
        DataFrame with compliance metrics
    """
    analyzers = [Compliance(name, pattern)]
    return run_analyzers(spark, df, analyzers)


def compute_count_by_pattern(spark: SparkSession, df: DataFrame, column: str,
                            pattern: str) -> DataFrame:
    """
    Count values matching a specific regex pattern.

    Args:
        spark: SparkSession instance
        df: Input DataFrame
        column: Column name
        pattern: Regex pattern to match

    Returns:
        DataFrame with pattern match metrics
    """
    analyzers = [PatternMatch(column, pattern)]
    return run_analyzers(spark, df, analyzers)


def compute_full_profile(spark: SparkSession, df: DataFrame) -> DataFrame:
    """
    Compute comprehensive profile across all columns.

    Args:
        spark: SparkSession instance
        df: Input DataFrame

    Returns:
        DataFrame with comprehensive profiling metrics
    """
    runner = AnalysisRunner(spark).onData(df)

    for col in df.columns:
        runner = runner.addAnalyzer(Completeness(col))
        runner = runner.addAnalyzer(CountDistinct(col))

    runner = runner.addAnalyzer(Size())

    numeric_cols = [f.name for f in df.schema.fields if f.dataType.typeName() in ['integer', 'long', 'double', 'float']]
    for col in numeric_cols:
        runner = runner.addAnalyzer(Mean(col))
        runner = runner.addAnalyzer(Minimum(col))
        runner = runner.addAnalyzer(Maximum(col))

    result = runner.run()
    return AnalyzerContext.successMetricsAsDataFrame(spark, result)


def compute_column_profile(spark: SparkSession, df: DataFrame, column: str) -> DataFrame:
    """
    Compute detailed profile for a single column.

    Args:
        spark: SparkSession instance
        df: Input DataFrame
        column: Column name

    Returns:
        DataFrame with column profiling metrics
    """
    runner = AnalysisRunner(spark).onData(df) \
        .addAnalyzer(Completeness(column)) \
        .addAnalyzer(CountDistinct(column))

    field_type = [f.dataType.typeName() for f in df.schema.fields if f.name == column][0]
    if field_type in ['integer', 'long', 'double', 'float']:
        runner = runner.addAnalyzer(Mean(column)) \
                       .addAnalyzer(Maximum(column)) \
                       .addAnalyzer(Minimum(column)) \
                       .addAnalyzer(StandardDeviation(column))

    result = runner.run()
    return AnalyzerContext.successMetricsAsDataFrame(spark, result)


# ==================================================================================
# SECTION 2: VERIFICATION & CONSTRAINT CHECKS
# ==================================================================================

def run_quality_checks(spark: SparkSession, df: DataFrame,
                      check_configs: List[Dict[str, Any]]) -> Any:
    """
    Run quality checks based on configurations.

    Args:
        spark: SparkSession instance
        df: Input DataFrame
        check_configs: List of check configurations
        Example:
            [
                {"column": "age", "constraint": "completeness"},
                {"column": "email", "constraint": "pattern", "pattern": ".*@.*\\.com$"},
                {"column": "age", "constraint": "range", "min_val": 0, "max_val": 120}
            ]

    Returns:
        VerificationResult object
    """
    check = Check(spark, CheckLevel.Warning, "Data Quality Check")

    for config in check_configs:
        constraint = config.get("constraint")
        column = config.get("column")

        if constraint == "completeness":
            check.isComplete(column)
        elif constraint == "non_negative":
            check.isNonNegative(column)
        elif constraint == "pattern":
            pattern = config.get("pattern")
            check.satisfies(f"`{column}` RLIKE '{pattern}'", f"{column} pattern check")
        elif constraint == "range":
            min_val = config.get("min_val", 0)
            max_val = config.get("max_val", 9999)
            check.satisfies(
                f"`{column}` >= {min_val} AND `{column}` <= {max_val}",
                f"{column} in range {min_val}-{max_val}"
            )
        elif constraint == "is_contained_in":
            values = config.get("values", [])
            check.isContainedIn(column, values)

    verification_result = VerificationSuite(spark).onData(df).addCheck(check).run()
    return verification_result


# ==================================================================================
# SECTION 3: PYTEST TEST CLASSES (ALIGNED WITH test_etl_file.py LOGIC)
# ==================================================================================

@allure.parent_suite("PyDeequ Data Quality Validation")
class TestPyDeequDataQuality:
    """Test suite for PyDeequ Data Quality - Aligned with ETL tests"""

    # ---------------------------
    # Record Count (PyDeequ Size)
    # ---------------------------
    def test_record_count_pydeequ(self, input_data):
        """Test record count using PyDeequ Size analyzer"""
        df, _, customer, dataset, _ = input_data
        spark = SparkSession.getActiveSession()

        allure.dynamic.feature(customer)
        allure.dynamic.story(dataset)
        allure.dynamic.title("Record Count (PyDeequ)")

        # Use PyDeequ Size analyzer
        result_df = run_analyzers(spark, df, [Size()])
        result_pd = result_df.toPandas()

        total_count = int(result_pd.iloc[0]['value']) if not result_pd.empty else 0

        allure.attach(
            str(total_count),
            name="Total Record Count",
            attachment_type=allure.attachment_type.TEXT
        )

        print("\n" + "=" * 100)
        print(f"RECORD COUNT: {customer} - {dataset}")
        print(f"Total Count: {total_count}")
        print("=" * 100)

        assert total_count > 0, f"{customer}-{dataset} is empty"

    # ---------------------------
    #  Schema Validation
    # ---------------------------
    def test_schema_validation_pydeequ(self, input_data):
        """Validate dataset columns against the ADP schema sheet"""
        df, base_df, customer, dataset, details = input_data

        if not has_schema_sheet(details):
            pytest.skip("No schema sheet configured")

        if base_df is None:
            pytest.skip(f"Schema file could not be read for {customer}-{dataset}")

        allure.dynamic.feature(customer)
        allure.dynamic.story(dataset)
        allure.dynamic.title("Schema Validation (PyDeequ)")

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
            for column in result["missing_columns"]:
                print(column)

        if result["unexpected_columns"]:
            print("\nUnexpected Columns:")
            for column in result["unexpected_columns"]:
                print(column)

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
    # Null Validation (PyDeequ Completeness)
    # ---------------------------
    def test_null_validation_pydeequ(self, input_data):
        """Test null validation using PyDeequ Completeness analyzer"""
        df, _, customer, dataset, _ = input_data
        spark = SparkSession.getActiveSession()

        allure.dynamic.feature(customer)
        allure.dynamic.story(dataset)
        allure.dynamic.title("Null Check (PyDeequ)")

        # Get null counts using PyDeequ
        null_counts = get_null_counts_pydeequ(spark, df)

        null_df = pd.DataFrame(
            list(null_counts.items()),
            columns=["Column Name", "Null Count"]
        )

        failed_df = null_df[null_df["Null Count"] > 0]

        # =====================================================
        # Attach full table
        # =====================================================
        allure.attach(
            null_df.to_html(index=False),
            name="Null Counts (All Columns)",
            attachment_type=allure.attachment_type.HTML
        )

        # =====================================================
        # Attach only failed columns
        # =====================================================
        if not failed_df.empty:
            allure.attach(
                failed_df.to_html(index=False),
                name="Columns with Nulls",
                attachment_type=allure.attachment_type.HTML
            )

            allure.attach(
                f"{customer}-{dataset} has null values",
                name="Warning",
                attachment_type=allure.attachment_type.TEXT
            )

            print(f"WARNING: {customer}-{dataset} has nulls")

        # =====================================================
        # Console Logs
        # =====================================================
        print("\n" + "=" * 100)
        print(f"Customer : {customer}")
        print(f"Dataset  : {dataset}")
        print("\nNull Counts by Column:")
        print(null_df.to_string(index=False))
        print("=" * 100)

        # Always pass - warn only
        assert True

    # ---------------------------
    # Duplicate Check (PyDeequ)
    # ---------------------------
    def test_duplicate_validation_pydeequ(self, input_data):
        """Test duplicate validation using PyDeequ"""
        df, _, customer, dataset, details = input_data
        spark = SparkSession.getActiveSession()

        allure.dynamic.feature(customer)
        allure.dynamic.story(dataset)
        allure.dynamic.title("Duplicate Check (PyDeequ)")

        keys = details.get("keys")

        # Skip if keys not defined
        if not keys:
            pytest.skip(f"No keys defined for {dataset}")

        # Check duplicates using PyDeequ
        result = check_composite_key_duplicates_pydeequ(spark, df, keys)
        dup_count = result["dup_count"]

        allure.attach(
            str(dup_count),
            name="Total Duplicate Key Count",
            attachment_type=allure.attachment_type.TEXT
        )

        # =====================================================
        # PyDeequ analyzer metrics the count was derived from.
        # PyDeequ reports metrics, not rows, so the offending keys
        # themselves cannot be attached here.
        # =====================================================
        allure.attach(
            result["metrics"].to_html(index=False),
            name="PyDeequ Uniqueness Metrics",
            attachment_type=allure.attachment_type.HTML
        )

        # =====================================================
        # Console Logs
        # =====================================================
        print("\n" + "=" * 100)
        print(f"Customer         : {customer}")
        print(f"Dataset          : {dataset}")
        print(f"Composite Keys   : {keys}")
        if result["is_current_col"]:
            print(f"is_current_col   : {result['is_current_col']} (scoped to current records)")
        print(f"Rows Scoped      : {result['size']}")
        print(f"Distinct Keys    : {result['distinct_keys']}")
        print(f"Keys Seen Once   : {result['unique_once']}")
        print(f"Duplicate Count  : {dup_count}")
        print("=" * 100)

        assert dup_count == 0, f"{customer}-{dataset} has duplicates: {dup_count}"

    # ---------------------------
    # Composite Key NULL Validation (PyDeequ)
    # ---------------------------
    def test_composite_key_null_validation_pydeequ(self, input_data):
        """Test composite key null validation using PyDeequ"""
        df, _, customer, dataset, details = input_data
        spark = SparkSession.getActiveSession()

        allure.dynamic.feature(customer)
        allure.dynamic.story(dataset)
        allure.dynamic.title("Composite Key NULL Validation (PyDeequ)")

        keys = details.get("keys")

        if not keys:
            pytest.skip(f"No keys defined for {dataset}")

        # Check nulls in composite keys using PyDeequ
        result = check_composite_key_nulls_pydeequ(
            spark,
            df,
            keys,
            ignore_values=["None", "NA", "N/A"]
        )

        # =====================================================
        # Reporting - Summary (TEXT, matching test_etl_file.py)
        # =====================================================
        allure.attach(
            f"Total Rows: {result['total_count']}\n"
            f"Ignored Rows: {result['ignored_count']}\n"
            f"Rows Checked: {result['checked_count']}\n"
            f"NULL Count: {result['null_count']}",
            name="Validation Summary",
            attachment_type=allure.attachment_type.TEXT
        )

        # =====================================================
        # PyDeequ analyzer metrics the counts were derived from.
        # PyDeequ reports metrics, not rows, so the offending rows
        # themselves cannot be attached here.
        # =====================================================
        allure.attach(
            result["metrics"].to_html(index=False),
            name="PyDeequ Compliance Metrics",
            attachment_type=allure.attachment_type.HTML
        )

        # =====================================================
        # Console Logs
        # =====================================================
        print("\n" + "=" * 100)
        print(f"Customer         : {customer}")
        print(f"Dataset          : {dataset}")
        print(f"Composite Keys   : {keys}")
        print(f"Total Rows       : {result['total_count']}")
        print(f"NULL Count       : {result['null_count']}")
        print("=" * 100)

        # =====================================================
        # Assertion
        # =====================================================
        assert result["null_count"] == 0, \
            f"{customer}-{dataset} has NULLs in keys: {result['null_count']}"

    # ---------------------------
    # Key Completeness (PyDeequ Completeness == 1.0)
    # ---------------------------
    def test_key_completeness_pydeequ(self, input_data):
        """Composite-key columns must be 100% complete (PyDeequ Completeness analyzer)."""
        df, _, customer, dataset, details = input_data
        spark = SparkSession.getActiveSession()

        allure.dynamic.feature(customer)
        allure.dynamic.story(dataset)
        allure.dynamic.title("Key Completeness (PyDeequ)")

        keys = details.get("keys")
        if not keys:
            pytest.skip(f"No keys defined for {dataset}")

        metrics_pd = compute_completeness(spark, df, keys).toPandas()

        summary_df = (
            metrics_pd[["instance", "value"]]
            .rename(columns={"instance": "Key Column", "value": "Completeness"})
        )
        incomplete = summary_df[summary_df["Completeness"] < 1.0]

        allure.attach(
            summary_df.to_html(index=False),
            name="Key Completeness Metrics",
            attachment_type=allure.attachment_type.HTML,
        )

        print("\n" + "=" * 100)
        print(f"Customer : {customer}")
        print(f"Dataset  : {dataset}")
        print(f"Keys     : {keys}")
        print(summary_df.to_string(index=False))
        print("=" * 100)

        assert incomplete.empty, (
            f"{customer}-{dataset} has incomplete key columns:\n"
            f"{incomplete.to_string(index=False)}"
        )

    # ---------------------------
    # Key Non-Negativity (PyDeequ VerificationSuite / Check)
    # ---------------------------
    def test_key_non_negative_pydeequ(self, input_data):
        """Numeric key columns (e.g. GEOGRAPHY_KEY, DATE_KEY) must be non-negative.

        Uses the native PyDeequ VerificationSuite constraint path rather than
        deriving the answer from analyzer metrics.
        """
        df, _, customer, dataset, details = input_data
        spark = SparkSession.getActiveSession()

        allure.dynamic.feature(customer)
        allure.dynamic.story(dataset)
        allure.dynamic.title("Key Non-Negativity (PyDeequ)")

        keys = details.get("keys")
        if not keys:
            pytest.skip(f"No keys defined for {dataset}")

        numeric_types = ("integer", "long", "double", "float", "decimal", "short", "byte")
        numeric_keys = [
            f.name for f in df.schema.fields
            if f.name in keys and f.dataType.typeName() in numeric_types
        ]

        if not numeric_keys:
            pytest.skip(f"No numeric key columns in {dataset}")

        check_configs = [{"column": c, "constraint": "non_negative"} for c in numeric_keys]
        verification_result = run_quality_checks(spark, df, check_configs)
        result_df = VerificationResult.checkResultsAsDataFrame(spark, verification_result).toPandas()

        allure.attach(
            result_df.to_html(index=False),
            name="PyDeequ Verification Results",
            attachment_type=allure.attachment_type.HTML,
        )

        failed = result_df[result_df["constraint_status"] != "Success"]

        print("\n" + "=" * 100)
        print(f"Customer     : {customer}")
        print(f"Dataset      : {dataset}")
        print(f"Numeric Keys : {numeric_keys}")
        print(result_df.to_string(index=False))
        print("=" * 100)

        assert failed.empty, (
            f"{customer}-{dataset} has negative values in key columns:\n"
            f"{failed.to_string(index=False)}"
        )

    # ---------------------------
    # is_current Flag Validation (PyDeequ Compliance)
    # ---------------------------
    def test_is_current_flag_pydeequ(self, input_data):
        """When an is_current column exists, every row should be current.

        Measured with PyDeequ Compliance so it works for boolean or string
        representations of the flag.
        """
        df, _, customer, dataset, _ = input_data
        spark = SparkSession.getActiveSession()

        allure.dynamic.feature(customer)
        allure.dynamic.story(dataset)
        allure.dynamic.title("is_current Flag (PyDeequ)")

        is_current_col = next(
            (c for c in df.columns if c.lower() == "is_current"),
            None,
        )
        if not is_current_col:
            pytest.skip(f"No is_current column in {dataset}")

        predicate = f"lower(cast(`{is_current_col}` as string)) = 'true'"
        metrics_pd = compute_compliance(
            spark, df, is_current_col, predicate, name="is_current_true"
        ).toPandas()

        current_ratio = float(metrics_pd.iloc[0]["value"]) if not metrics_pd.empty else 0.0

        allure.attach(
            metrics_pd.to_html(index=False),
            name="PyDeequ is_current Compliance",
            attachment_type=allure.attachment_type.HTML,
        )

        print("\n" + "=" * 100)
        print(f"Customer        : {customer}")
        print(f"Dataset         : {dataset}")
        print(f"is_current col  : {is_current_col}")
        print(f"Current Ratio   : {current_ratio}")
        print("=" * 100)

        assert current_ratio == 1.0, (
            f"{customer}-{dataset} has non-current rows in {is_current_col}: "
            f"{round((1.0 - current_ratio) * 100, 4)}% not current"
        )

    # ---------------------------
    # Numeric Column Summary (PyDeequ) - profiling / warn-only
    # ---------------------------
    def test_numeric_summary_pydeequ(self, input_data):
        """Profile numeric columns (mean/stddev/min/max/sum) via PyDeequ. Warn-only."""
        df, _, customer, dataset, _ = input_data
        spark = SparkSession.getActiveSession()

        allure.dynamic.feature(customer)
        allure.dynamic.story(dataset)
        allure.dynamic.title("Numeric Column Summary (PyDeequ)")

        numeric_types = ("integer", "long", "double", "float", "decimal", "short", "byte")
        numeric_cols = [
            f.name for f in df.schema.fields
            if f.dataType.typeName() in numeric_types
        ]

        if not numeric_cols:
            pytest.skip(f"No numeric columns in {dataset}")

        metrics_pd = compute_numeric_summary(spark, df, numeric_cols).toPandas()

        allure.attach(
            metrics_pd.to_html(index=False),
            name="PyDeequ Numeric Summary",
            attachment_type=allure.attachment_type.HTML,
        )

        print("\n" + "=" * 100)
        print(f"Customer        : {customer}")
        print(f"Dataset         : {dataset}")
        print(f"Numeric Columns : {numeric_cols}")
        print(metrics_pd.to_string(index=False))
        print("=" * 100)

        # Profiling only - never fails the suite.
        assert True

    # ---------------------------
    # Column Completeness Profile (PyDeequ) - profiling / warn-only
    # ---------------------------
    def test_column_completeness_profile_pydeequ(self, input_data):
        """Completeness of every column via PyDeequ. Warn-only, flags empty columns."""
        df, _, customer, dataset, _ = input_data
        spark = SparkSession.getActiveSession()

        allure.dynamic.feature(customer)
        allure.dynamic.story(dataset)
        allure.dynamic.title("Column Completeness Profile (PyDeequ)")

        metrics_pd = compute_completeness(spark, df, df.columns).toPandas()

        profile_df = (
            metrics_pd[["instance", "value"]]
            .rename(columns={"instance": "Column", "value": "Completeness"})
            .sort_values("Completeness")
            .reset_index(drop=True)
        )
        empty_cols = profile_df[profile_df["Completeness"] == 0.0]

        allure.attach(
            profile_df.to_html(index=False),
            name="Column Completeness (All Columns)",
            attachment_type=allure.attachment_type.HTML,
        )

        if not empty_cols.empty:
            allure.attach(
                empty_cols.to_html(index=False),
                name="Fully Empty Columns",
                attachment_type=allure.attachment_type.HTML,
            )

        print("\n" + "=" * 100)
        print(f"Customer : {customer}")
        print(f"Dataset  : {dataset}")
        print(profile_df.to_string(index=False))
        if not empty_cols.empty:
            print(f"\nFully empty columns ({len(empty_cols)}):")
            print(empty_cols["Column"].to_string(index=False))
        print("=" * 100)

        # Profiling only - never fails the suite.
        assert True

    # =====================================================================
    # SCHEMA-SHEET DRIVEN PYDEEQU TESTS
    # These need the ADP schema sheet configured (details["sheet_name"]).
    # They skip cleanly when no sheet is configured or the file is unreadable,
    # exactly like test_schema_validation_pydeequ.
    # =====================================================================

    @staticmethod
    def _expected_columns_from_schema(base_df, column_name="Standardize Column Name"):
        """ADP expected column names (normalized to lower-case), from the schema sheet."""
        return set(
            base_df[column_name]
            .dropna()
            .astype(str)
            .str.strip()
            .str.replace("\n", " ", regex=False)
            .str.lower()
            .tolist()
        )

    # ---------------------------
    # ADP Expected-Column Completeness (PyDeequ Completeness)
    # ---------------------------
    def test_expected_columns_completeness_pydeequ(self, input_data):
        """Completeness of the ADP-defined columns, measured with PyDeequ.

        Requires the schema sheet so we know WHICH columns ADP expects; the
        measurement itself is 100% PyDeequ (Completeness analyzer). Warn-only.
        """
        df, base_df, customer, dataset, details = input_data

        if not has_schema_sheet(details):
            pytest.skip("No schema sheet configured")

        if base_df is None:
            pytest.skip(f"Schema file could not be read for {customer}-{dataset}")

        spark = SparkSession.getActiveSession()

        allure.dynamic.feature(customer)
        allure.dynamic.story(dataset)
        allure.dynamic.title("ADP Column Completeness (PyDeequ)")

        expected_cols = self._expected_columns_from_schema(base_df)

        # Map normalized ADP names back to the dataset's real column names.
        actual_map = {
            c.strip().replace("\n", " ").lower(): c for c in df.columns
        }
        overlap = [actual_map[e] for e in expected_cols if e in actual_map]

        if not overlap:
            pytest.skip(f"No ADP-expected columns present in {dataset}")

        metrics_pd = compute_completeness(spark, df, overlap).toPandas()

        profile_df = (
            metrics_pd[["instance", "value"]]
            .rename(columns={"instance": "ADP Column", "value": "Completeness"})
            .sort_values("Completeness")
            .reset_index(drop=True)
        )
        empty_cols = profile_df[profile_df["Completeness"] == 0.0]

        allure.attach(
            profile_df.to_html(index=False),
            name="ADP Column Completeness (PyDeequ)",
            attachment_type=allure.attachment_type.HTML,
        )
        if not empty_cols.empty:
            allure.attach(
                empty_cols.to_html(index=False),
                name="Empty ADP Columns",
                attachment_type=allure.attachment_type.HTML,
            )

        print("\n" + "=" * 100)
        print(f"Customer          : {customer}")
        print(f"Dataset           : {dataset}")
        print(f"ADP Columns Found : {len(overlap)}")
        print(profile_df.to_string(index=False))
        print("=" * 100)

        # Profiling only - never fails the suite.
        assert True

    # ---------------------------
    # ADP Key-Column Contract + Completeness (PyDeequ Completeness)
    # ---------------------------
    def test_adp_key_columns_completeness_pydeequ(self, input_data):
        """Configured keys must belong to the ADP schema AND be 100% complete.

        The schema-contract part (keys are ADP-recognized) needs the sheet;
        the completeness measurement is 100% PyDeequ.
        """
        df, base_df, customer, dataset, details = input_data

        if not has_schema_sheet(details):
            pytest.skip("No schema sheet configured")

        if base_df is None:
            pytest.skip(f"Schema file could not be read for {customer}-{dataset}")

        keys = details.get("keys")
        if not keys:
            pytest.skip(f"No keys defined for {dataset}")

        spark = SparkSession.getActiveSession()

        allure.dynamic.feature(customer)
        allure.dynamic.story(dataset)
        allure.dynamic.title("ADP Key Contract + Completeness (PyDeequ)")

        expected_cols = self._expected_columns_from_schema(base_df)

        keys_not_in_adp = [
            k for k in keys
            if k.strip().replace("\n", " ").lower() not in expected_cols
        ]

        metrics_pd = compute_completeness(spark, df, keys).toPandas()
        completeness_df = (
            metrics_pd[["instance", "value"]]
            .rename(columns={"instance": "Key Column", "value": "Completeness"})
        )
        incomplete = completeness_df[completeness_df["Completeness"] < 1.0]

        allure.attach(
            completeness_df.to_html(index=False),
            name="Key Completeness (PyDeequ)",
            attachment_type=allure.attachment_type.HTML,
        )
        if keys_not_in_adp:
            allure.attach(
                "\n".join(keys_not_in_adp),
                name="Keys Not Found In ADP Schema",
                attachment_type=allure.attachment_type.TEXT,
            )

        print("\n" + "=" * 100)
        print(f"Customer          : {customer}")
        print(f"Dataset           : {dataset}")
        print(f"Keys              : {keys}")
        print(f"Keys Not In ADP   : {keys_not_in_adp}")
        print(completeness_df.to_string(index=False))
        print("=" * 100)

        assert not keys_not_in_adp, (
            f"{customer}-{dataset} keys missing from ADP schema: {keys_not_in_adp}"
        )
        assert incomplete.empty, (
            f"{customer}-{dataset} has incomplete key columns:\n"
            f"{incomplete.to_string(index=False)}"
        )

    # =====================================================================
    # ADDITIONAL PROFILING TESTS (exercise previously-unused PyDeequ helpers)
    # All measurements below are 100% PyDeequ analyzers. They are warn-only
    # profiling, so they never fail the suite - they surface the shape of the
    # star_geo_dim / star_time_dim data in the Allure report.
    # =====================================================================

    _NUMERIC_TYPES = ("integer", "long", "double", "float", "decimal", "short", "byte")

    # ---------------------------
    # Composite-Key Uniqueness (PyDeequ Uniqueness)
    # ---------------------------
    def test_composite_key_uniqueness_pydeequ(self, input_data):
        """Combined uniqueness ratio of the composite key (PyDeequ Uniqueness).

        For star_geo_dim the key is GEOGRAPHY_KEY+SOURCE_SYSTEM and for
        star_time_dim it is DATE_KEY+SOURCE; the same id repeats once per
        source system, so only the COMBINED key is expected to be unique.
        Warn-only: the hard duplicate assertion lives in
        test_duplicate_validation_pydeequ.
        """
        df, _, customer, dataset, details = input_data
        spark = SparkSession.getActiveSession()

        allure.dynamic.feature(customer)
        allure.dynamic.story(dataset)
        allure.dynamic.title("Composite Key Uniqueness (PyDeequ)")

        keys = details.get("keys")
        if not keys:
            pytest.skip(f"No keys defined for {dataset}")

        metrics_pd = compute_uniqueness(spark, df, keys, combined=True).toPandas()

        summary_df = (
            metrics_pd[["instance", "value"]]
            .rename(columns={"instance": "Composite Key", "value": "Uniqueness"})
        )

        allure.attach(
            summary_df.to_html(index=False),
            name="Composite Key Uniqueness (PyDeequ)",
            attachment_type=allure.attachment_type.HTML,
        )

        print("\n" + "=" * 100)
        print(f"Customer : {customer}")
        print(f"Dataset  : {dataset}")
        print(f"Keys     : {keys}")
        print(summary_df.to_string(index=False))
        print("=" * 100)

        # Profiling only - never fails the suite.
        assert True

    # ---------------------------
    # Per-Column Distinctness (PyDeequ Distinctness)
    # ---------------------------
    def test_key_distinctness_pydeequ(self, input_data):
        """Distinctness of each key column individually (PyDeequ Distinctness).

        A low distinctness on a key column (e.g. SOURCE_SYSTEM has only
        DUKES/KERNEL/MATEOS) is expected; this profiles it rather than
        asserting on it. Warn-only.
        """
        df, _, customer, dataset, details = input_data
        spark = SparkSession.getActiveSession()

        allure.dynamic.feature(customer)
        allure.dynamic.story(dataset)
        allure.dynamic.title("Key Column Distinctness (PyDeequ)")

        keys = details.get("keys")
        if not keys:
            pytest.skip(f"No keys defined for {dataset}")

        metrics_pd = compute_distinctness(spark, df, keys, individual=True).toPandas()

        summary_df = (
            metrics_pd[["instance", "value"]]
            .rename(columns={"instance": "Key Column", "value": "Distinctness"})
            .sort_values("Distinctness")
            .reset_index(drop=True)
        )

        allure.attach(
            summary_df.to_html(index=False),
            name="Key Column Distinctness (PyDeequ)",
            attachment_type=allure.attachment_type.HTML,
        )

        print("\n" + "=" * 100)
        print(f"Customer : {customer}")
        print(f"Dataset  : {dataset}")
        print(summary_df.to_string(index=False))
        print("=" * 100)

        # Profiling only - never fails the suite.
        assert True

    # ---------------------------
    # Categorical Entropy (PyDeequ Entropy)
    # ---------------------------
    def test_categorical_entropy_pydeequ(self, input_data):
        """Entropy (value diversity) of string/categorical columns (PyDeequ Entropy).

        Meaningful for columns like DAY_OF_WEEK_NAME, CALENDAR_MONTH_NAME,
        SOURCE and SOURCE_SYSTEM. Fully-null columns produce no metric and are
        simply absent from the report. Warn-only.
        """
        df, _, customer, dataset, _ = input_data
        spark = SparkSession.getActiveSession()

        allure.dynamic.feature(customer)
        allure.dynamic.story(dataset)
        allure.dynamic.title("Categorical Entropy (PyDeequ)")

        string_cols = [
            f.name for f in df.schema.fields
            if f.dataType.typeName() == "string"
        ]
        if not string_cols:
            pytest.skip(f"No string columns in {dataset}")

        metrics_pd = compute_entropy(spark, df, string_cols).toPandas()
        if metrics_pd.empty:
            pytest.skip(f"No entropy metrics produced for {dataset} (all-null columns)")

        summary_df = (
            metrics_pd[["instance", "value"]]
            .rename(columns={"instance": "Column", "value": "Entropy"})
            .sort_values("Entropy", ascending=False)
            .reset_index(drop=True)
        )

        allure.attach(
            summary_df.to_html(index=False),
            name="Categorical Entropy (PyDeequ)",
            attachment_type=allure.attachment_type.HTML,
        )

        print("\n" + "=" * 100)
        print(f"Customer : {customer}")
        print(f"Dataset  : {dataset}")
        print(summary_df.to_string(index=False))
        print("=" * 100)

        # Profiling only - never fails the suite.
        assert True

    # ---------------------------
    # Date Pattern Match (PyDeequ PatternMatch)
    # ---------------------------
    def test_date_pattern_match_pydeequ(self, input_data):
        """Fraction of a date-like column matching YYYY-MM-DD (PyDeequ PatternMatch).

        Targets columns such as CALENDAR_DATE / NEXT_CALENDAR_DATE in
        star_time_dim. Skips cleanly when no populated date-like string column
        is present (star_geo_dim has none). Warn-only.
        """
        df, _, customer, dataset, _ = input_data
        spark = SparkSession.getActiveSession()

        allure.dynamic.feature(customer)
        allure.dynamic.story(dataset)
        allure.dynamic.title("Date Pattern Match (PyDeequ)")

        date_col = next(
            (
                f.name for f in df.schema.fields
                if f.dataType.typeName() == "string" and "DATE" in f.name.upper()
            ),
            None,
        )
        if not date_col:
            pytest.skip(f"No date-like string column in {dataset}")

        pattern = r"\d{4}-\d{2}-\d{2}"
        metrics_pd = compute_count_by_pattern(spark, df, date_col, pattern).toPandas()
        if metrics_pd.empty:
            pytest.skip(f"No PatternMatch metric produced for {date_col} in {dataset}")

        match_ratio = float(metrics_pd.iloc[0]["value"])

        summary_df = (
            metrics_pd[["instance", "value"]]
            .rename(columns={"instance": "Column", "value": "Match Ratio"})
        )

        allure.attach(
            summary_df.to_html(index=False),
            name="Date Pattern Match (PyDeequ)",
            attachment_type=allure.attachment_type.HTML,
        )

        print("\n" + "=" * 100)
        print(f"Customer    : {customer}")
        print(f"Dataset     : {dataset}")
        print(f"Date Column : {date_col}")
        print(f"Match Ratio : {match_ratio}")
        print("=" * 100)

        # Profiling only - never fails the suite.
        assert True

    # ---------------------------
    # Numeric Standard Deviation (PyDeequ StandardDeviation)
    # ---------------------------
    def test_numeric_stddev_pydeequ(self, input_data):
        """Standard deviation of numeric columns (PyDeequ StandardDeviation).

        Uses the dedicated compute_standard_deviation helper. Numeric columns
        include GEOGRAPHY_KEY / DATE_KEY / CALENDAR_YEAR / CALENDAR_MONTH etc.
        Warn-only.
        """
        df, _, customer, dataset, _ = input_data
        spark = SparkSession.getActiveSession()

        allure.dynamic.feature(customer)
        allure.dynamic.story(dataset)
        allure.dynamic.title("Numeric Standard Deviation (PyDeequ)")

        numeric_cols = [
            f.name for f in df.schema.fields
            if f.dataType.typeName() in self._NUMERIC_TYPES
        ]
        if not numeric_cols:
            pytest.skip(f"No numeric columns in {dataset}")

        metrics_pd = compute_standard_deviation(spark, df, numeric_cols).toPandas()
        if metrics_pd.empty:
            pytest.skip(f"No StandardDeviation metrics produced for {dataset}")

        summary_df = (
            metrics_pd[["instance", "value"]]
            .rename(columns={"instance": "Column", "value": "StdDev"})
            .sort_values("Column")
            .reset_index(drop=True)
        )

        allure.attach(
            summary_df.to_html(index=False),
            name="Numeric Standard Deviation (PyDeequ)",
            attachment_type=allure.attachment_type.HTML,
        )

        print("\n" + "=" * 100)
        print(f"Customer        : {customer}")
        print(f"Dataset         : {dataset}")
        print(f"Numeric Columns : {numeric_cols}")
        print(summary_df.to_string(index=False))
        print("=" * 100)

        # Profiling only - never fails the suite.
        assert True

    # ---------------------------
    # Full Dataset Profile (PyDeequ AnalysisRunner)
    # ---------------------------
    def test_full_profile_pydeequ(self, input_data):
        """Comprehensive profile across all columns (PyDeequ compute_full_profile).

        Completeness + CountDistinct for every column, plus Mean/Min/Max for
        numeric columns and overall Size, in a single analyzer run. Warn-only.
        """
        df, _, customer, dataset, _ = input_data
        spark = SparkSession.getActiveSession()

        allure.dynamic.feature(customer)
        allure.dynamic.story(dataset)
        allure.dynamic.title("Full Dataset Profile (PyDeequ)")

        metrics_pd = compute_full_profile(spark, df).toPandas()
        if metrics_pd.empty:
            pytest.skip(f"No profile metrics produced for {dataset}")

        profile_df = (
            metrics_pd[["entity", "instance", "name", "value"]]
            .sort_values(["name", "instance"])
            .reset_index(drop=True)
        )

        allure.attach(
            profile_df.to_html(index=False),
            name="Full Dataset Profile (PyDeequ)",
            attachment_type=allure.attachment_type.HTML,
        )

        print("\n" + "=" * 100)
        print(f"Customer : {customer}")
        print(f"Dataset  : {dataset}")
        print(f"Metrics  : {len(profile_df)}")
        print(profile_df.to_string(index=False))
        print("=" * 100)

        # Profiling only - never fails the suite.
        assert True

    # ---------------------------
    # Single Key Column Profile (PyDeequ AnalysisRunner)
    # ---------------------------
    def test_single_column_profile_pydeequ(self, input_data):
        """Detailed profile of the first key column (PyDeequ compute_column_profile).

        Completeness + CountDistinct, plus Mean/Min/Max/StdDev when the column
        is numeric (GEOGRAPHY_KEY / DATE_KEY). Warn-only.
        """
        df, _, customer, dataset, details = input_data
        spark = SparkSession.getActiveSession()

        allure.dynamic.feature(customer)
        allure.dynamic.story(dataset)
        allure.dynamic.title("Single Column Profile (PyDeequ)")

        keys = details.get("keys")
        column = keys[0] if keys else (df.columns[0] if df.columns else None)
        if not column:
            pytest.skip(f"No column available to profile in {dataset}")

        metrics_pd = compute_column_profile(spark, df, column).toPandas()
        if metrics_pd.empty:
            pytest.skip(f"No profile metrics produced for {column} in {dataset}")

        profile_df = (
            metrics_pd[["name", "value"]]
            .rename(columns={"name": "Metric", "value": "Value"})
            .reset_index(drop=True)
        )

        allure.attach(
            profile_df.to_html(index=False),
            name=f"Column Profile - {column} (PyDeequ)",
            attachment_type=allure.attachment_type.HTML,
        )

        print("\n" + "=" * 100)
        print(f"Customer : {customer}")
        print(f"Dataset  : {dataset}")
        print(f"Column   : {column}")
        print(profile_df.to_string(index=False))
        print("=" * 100)

        # Profiling only - never fails the suite.
        assert True

    # ---------------------------
    # ADP Column Distinctness (schema-sheet + PyDeequ Distinctness)
    # ---------------------------
    def test_adp_column_distinctness_pydeequ(self, input_data):
        """Distinctness of the ADP-defined columns (PyDeequ Distinctness).

        Requires the ADP schema sheet to know WHICH columns to profile; the
        measurement itself is 100% PyDeequ. Skips cleanly when no sheet is
        configured or the file is unreadable, exactly like
        test_schema_validation_pydeequ. Warn-only.
        """
        df, base_df, customer, dataset, details = input_data

        if not has_schema_sheet(details):
            pytest.skip("No schema sheet configured")

        if base_df is None:
            pytest.skip(f"Schema file could not be read for {customer}-{dataset}")

        spark = SparkSession.getActiveSession()

        allure.dynamic.feature(customer)
        allure.dynamic.story(dataset)
        allure.dynamic.title("ADP Column Distinctness (PyDeequ)")

        expected_cols = self._expected_columns_from_schema(base_df)

        actual_map = {
            c.strip().replace("\n", " ").lower(): c for c in df.columns
        }
        overlap = [actual_map[e] for e in expected_cols if e in actual_map]
        if not overlap:
            pytest.skip(f"No ADP-expected columns present in {dataset}")

        metrics_pd = compute_distinctness(spark, df, overlap, individual=True).toPandas()
        if metrics_pd.empty:
            pytest.skip(f"No distinctness metrics produced for {dataset}")

        summary_df = (
            metrics_pd[["instance", "value"]]
            .rename(columns={"instance": "ADP Column", "value": "Distinctness"})
            .sort_values("Distinctness")
            .reset_index(drop=True)
        )

        allure.attach(
            summary_df.to_html(index=False),
            name="ADP Column Distinctness (PyDeequ)",
            attachment_type=allure.attachment_type.HTML,
        )

        print("\n" + "=" * 100)
        print(f"Customer          : {customer}")
        print(f"Dataset           : {dataset}")
        print(f"ADP Columns Found : {len(overlap)}")
        print(summary_df.to_string(index=False))
        print("=" * 100)

        # Profiling only - never fails the suite.
        assert True


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
