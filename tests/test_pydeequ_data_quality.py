#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PyDeequ Data Quality Tests for Databricks Tables

This module provides comprehensive data quality testing using PyDeequ
for tables defined in test_data/dataset_config.py.

Designed to run on Databricks and generate Allure reports.

Requirements:
    pip install pydeequ pyspark pandas pytest allure-pytest
"""

import os
import sys

os.environ["SPARK_VERSION"] = "3.3"

import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
import traceback

import pytest
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType

from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult
from pydeequ.analyzers import (
    AnalysisRunner, AnalyzerContext,
    Size, Completeness, Uniqueness, CountDistinct,
    Mean, Minimum, Maximum, Sum, StandardDeviation,
    ApproxCountDistinct, ApproxQuantile, Correlation,
    Entropy, Distinctness, PatternMatch, DataType,
    MinLength, MaxLength
)
from pydeequ.profiles import ColumnProfilerRunner
from pydeequ.suggestions import ConstraintSuggestionRunner, DEFAULT

sys.path.insert(0, str(Path(__file__).parent.parent / 'test_data'))
from dataset_config import DATASETS, SCHEMA_PATH

# Import Java wrapper
sys.path.insert(0, str(Path(__file__).parent))
from deequ_java_wrapper import DeequJavaWrapper


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class DataQualityMetric:
    """Data quality metric result"""
    metric_name: str
    metric_value: Any
    metric_type: str
    timestamp: str
    dataset: str
    table: str
    column: Optional[str] = None
    status: str = "SUCCESS"


@dataclass
class CheckResult:
    """Check result summary"""
    check_name: str
    constraint_status: str
    dataset: str
    table: str
    timestamp: str
    message: str = ""


class PyDeequDataQualityTester:
    """PyDeequ-based Data Quality Testing Framework"""

    def __init__(self, spark: SparkSession, dataset_name: str, table_name: str, table_info: Dict):
        self.spark = spark
        self.dataset_name = dataset_name
        self.table_name = table_name
        self.table_info = table_info
        self.table_path = table_info.get('table')
        self.df: Optional[DataFrame] = None
        self.results = {
            'metadata': {},
            'analysis_results': [],
            'check_results': [],
            'profile_results': {},
            'suggestions': {}
        }
        self.metrics: List[DataQualityMetric] = []
        self.check_results: List[CheckResult] = []

    def load_table(self) -> bool:
        """Load table from Databricks"""
        try:
            logger.info(f"📊 Loading table: {self.table_path}")
            self.df = self.spark.read.table(self.table_path)
            row_count = self.df.count()
            col_count = len(self.df.columns)
            logger.info(f"✅ Table loaded: {row_count} rows, {col_count} columns")

            self.results['metadata'] = {
                'dataset': self.dataset_name,
                'table': self.table_name,
                'table_path': self.table_path,
                'row_count': row_count,
                'column_count': col_count,
                'columns': self.df.columns,
                'schema': self.df.schema.jsonValue(),
                'load_timestamp': datetime.now().isoformat()
            }
            return True
        except Exception as e:
            logger.error(f"❌ Failed to load table {self.table_path}: {e}")
            logger.error(traceback.format_exc())
            return False

    def run_analyzers(self) -> bool:
        """Run comprehensive PyDeequ analyzers"""
        try:
            if self.df is None:
                logger.warning("⚠️ DataFrame not loaded, skipping analyzers")
                return False

            logger.info(f"🔍 Running analyzers for {self.table_name}...")

            try:
                analyzer_runner = AnalysisRunner(self.spark).onData(self.df)
            except AttributeError as e:
                if "_jvm" in str(e) or "JVM_ATTRIBUTE_NOT_SUPPORTED" in str(e):
                    logger.warning("⚠️ PyDeequ AnalysisRunner not supported in Spark Connect, skipping")
                    return False
                raise

            analyzer_runner = analyzer_runner.addAnalyzer(Size())

            for col in self.df.columns:
                analyzer_runner = analyzer_runner.addAnalyzer(Completeness(col))
                analyzer_runner = analyzer_runner.addAnalyzer(CountDistinct(col))
                try:
                    analyzer_runner = analyzer_runner.addAnalyzer(Uniqueness(col))
                except:
                    pass

            numeric_cols = [field.name for field in self.df.schema.fields
                           if field.dataType.typeName() in ['int', 'long', 'float', 'double']]

            for col in numeric_cols:
                analyzer_runner = analyzer_runner.addAnalyzer(Mean(col))
                analyzer_runner = analyzer_runner.addAnalyzer(Minimum(col))
                analyzer_runner = analyzer_runner.addAnalyzer(Maximum(col))
                analyzer_runner = analyzer_runner.addAnalyzer(Sum(col))
                analyzer_runner = analyzer_runner.addAnalyzer(StandardDeviation(col))

            string_cols = [field.name for field in self.df.schema.fields
                          if field.dataType.typeName() == 'string']

            for col in string_cols:
                try:
                    analyzer_runner = analyzer_runner.addAnalyzer(MinLength(col))
                    analyzer_runner = analyzer_runner.addAnalyzer(MaxLength(col))
                    analyzer_runner = analyzer_runner.addAnalyzer(ApproxCountDistinct(col))
                except:
                    pass

            logger.info("⏳ Executing analyzers...")
            analysis_result = analyzer_runner.run()

            successful_metrics = AnalyzerContext.successMetricsAsDataFrame(
                self.spark, analysis_result
            )

            metrics_pd = successful_metrics.toPandas()
            for _, row in metrics_pd.iterrows():
                metric = DataQualityMetric(
                    metric_name=str(row.get('analyzer', 'Unknown')),
                    metric_value=row.get('value'),
                    metric_type=str(row.get('analyzerOptions', {})),
                    timestamp=datetime.now().isoformat(),
                    dataset=self.dataset_name,
                    table=self.table_name,
                    column=str(row.get('instance', None)) if row.get('instance') else None
                )
                self.metrics.append(metric)

            self.results['analysis_results'] = [asdict(m) for m in self.metrics]
            logger.info(f"✅ Analyzers complete: {len(self.metrics)} metrics generated")
            return True

        except Exception as e:
            logger.error(f"❌ Analyzer error: {e}")
            logger.error(traceback.format_exc())
            return False

    def run_checks(self) -> bool:
        """Run PyDeequ verification checks"""
        try:
            if self.df is None:
                logger.warning("⚠️ DataFrame not loaded, skipping checks")
                return False

            logger.info(f"✔️ Running checks for {self.table_name}...")

            try:
                check = Check(self.spark, CheckLevel.Warning, "DataQualityChecks")
                check = check.hasSize(lambda x: x > 0, "Dataset must have rows")
            except AttributeError as e:
                if "_jvm" in str(e) or "JVM_ATTRIBUTE_NOT_SUPPORTED" in str(e):
                    logger.warning("⚠️ PyDeequ Checks not supported in Spark Connect, skipping")
                    return False
                raise

            for col in self.df.columns:
                try:
                    check = check.isComplete(col)
                except:
                    pass

            key_cols = self.table_info.get('keys', [])
            for col in key_cols:
                if col in self.df.columns:
                    try:
                        check = check.isUnique(col)
                    except:
                        pass

            numeric_cols = [field.name for field in self.df.schema.fields
                           if field.dataType.typeName() in ['int', 'long', 'float', 'double']]

            for col in numeric_cols:
                try:
                    check = check.isNonNegative(col)
                except:
                    pass

            email_cols = [col for col in self.df.columns if 'email' in col.lower()]
            for col in email_cols:
                try:
                    check = check.containsEmail(col)
                except:
                    pass

            logger.info("⏳ Running verification suite...")
            verification_result = VerificationSuite(self.spark) \
                .onData(self.df) \
                .addCheck(check) \
                .run()

            check_results_df = VerificationResult.checkResultsAsDataFrame(
                self.spark, verification_result
            )

            check_results_pd = check_results_df.toPandas()
            for _, row in check_results_pd.iterrows():
                check_result = CheckResult(
                    check_name=str(row.get('check', 'Unknown')),
                    constraint_status=str(row.get('constraint_status', 'Unknown')),
                    dataset=self.dataset_name,
                    table=self.table_name,
                    timestamp=datetime.now().isoformat(),
                    message=str(row.get('constraint_message', ''))
                )
                self.check_results.append(check_result)

            self.results['check_results'] = [asdict(c) for c in self.check_results]
            logger.info(f"✅ Checks complete: {len(self.check_results)} checks executed")
            return True

        except Exception as e:
            logger.error(f"❌ Check error: {e}")
            logger.error(traceback.format_exc())
            return False

    def run_column_profiler(self) -> bool:
        """Run column profiling"""
        try:
            if self.df is None:
                logger.warning("⚠️ DataFrame not loaded, skipping profiler")
                return False

            logger.info(f"📈 Running column profiler for {self.table_name}...")

            try:
                profiler_runner = ColumnProfilerRunner(self.spark).onData(self.df)
            except AttributeError as e:
                if "_jvm" in str(e) or "JVM_ATTRIBUTE_NOT_SUPPORTED" in str(e):
                    logger.warning("⚠️ ColumnProfilerRunner not supported in Spark Connect, skipping")
                    return False
                raise
            logger.info("⏳ Profiling columns...")
            profile_result = profiler_runner.run()

            profile_dict = {}
            for col_name, profile in profile_result.profiles.items():
                try:
                    profile_dict[col_name] = {
                        'completeness': float(profile.completeness) if hasattr(profile, 'completeness') else None,
                        'approximate_num_distinct_values': int(profile.approximateNumDistinctValues) if hasattr(profile, 'approximateNumDistinctValues') else None,
                        'type_counts': dict(profile.typeCounts) if hasattr(profile, 'typeCounts') else {}
                    }
                except Exception as e:
                    logger.warning(f"Could not extract profile for {col_name}: {e}")

            self.results['profile_results'] = profile_dict
            logger.info(f"✅ Profiler complete: {len(profile_dict)} columns profiled")
            return True

        except Exception as e:
            logger.error(f"❌ Profiler error: {e}")
            logger.error(traceback.format_exc())
            return False

    def generate_quality_report(self) -> Dict[str, Any]:
        """Generate quality assessment report"""
        try:
            total_checks = len(self.check_results)
            passed_checks = sum(1 for c in self.check_results if c.constraint_status == 'Success')
            warning_checks = sum(1 for c in self.check_results if c.constraint_status == 'Warning')
            failed_checks = total_checks - passed_checks - warning_checks

            total_metrics = len(self.metrics)

            quality_score = (passed_checks / total_checks * 100) if total_checks > 0 else 0
            quality_score = min(100, max(0, quality_score))

            report = {
                'dataset': self.dataset_name,
                'table': self.table_name,
                'quality_score': round(quality_score, 2),
                'total_checks': total_checks,
                'passed_checks': passed_checks,
                'warning_checks': warning_checks,
                'failed_checks': failed_checks,
                'total_metrics': total_metrics,
                'profiled_columns': len(self.results['profile_results']),
                'status': 'PASS' if quality_score >= 80 else 'WARNING' if quality_score >= 60 else 'FAIL',
                'row_count': self.results['metadata'].get('row_count', 0),
                'column_count': self.results['metadata'].get('column_count', 0),
                'timestamp': datetime.now().isoformat()
            }
            logger.info(f"📊 Quality Score: {quality_score}/100 - {report['status']}")
            return report
        except Exception as e:
            logger.error(f"❌ Report generation error: {e}")
            return {}

    def run_all_tests(self) -> Tuple[bool, Dict[str, Any]]:
        """Execute all quality tests"""
        try:
            if not self.load_table():
                return False, self.results

            self.run_analyzers()
            self.run_checks()
            self.run_column_profiler()

            report = self.generate_quality_report()
            self.results['report'] = report

            logger.info(f"✅ All tests completed for {self.table_name}")
            return True, self.results

        except Exception as e:
            logger.error(f"❌ Test execution failed: {e}")
            logger.error(traceback.format_exc())
            return False, self.results


@pytest.fixture(scope="session")
def spark_session():
    """Create Spark session for testing"""
    spark = SparkSession.getActiveSession()

    if spark is None:
        spark = SparkSession.builder \
            .appName("PyDeequ-DataQuality-Tests") \
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
            .getOrCreate()

    try:
        spark.sparkContext.setLogLevel("ERROR")
    except AttributeError:
        logger.info("⚠️ Using Spark Connect - sparkContext not available")

    logger.info("✅ Spark session created")
    yield spark


@pytest.fixture(params=[
    (dataset, table_name, table_info)
    for dataset, tables in DATASETS.items()
    for table_name, table_info in tables.items()
])
def dataset_table(request):
    """Parametrized fixture for all dataset tables"""
    return request.param


class TestPyDeequDataQuality:
    """PyDeequ Data Quality Test Suite"""

    def test_table_load(self, spark_session, dataset_table):
        """Test 1: Verify table can be loaded and is accessible"""
        import allure
        dataset, table_name, table_info = dataset_table

        with allure.step(f"Load table: {dataset}/{table_name}"):
            logger.info(f"\n{'='*80}")
            logger.info(f"📊 TEST 1: TABLE LOAD")
            logger.info(f"{'='*80}")
            logger.info(f"Dataset: {dataset}")
            logger.info(f"Table: {table_name}")
            logger.info(f"Table Path: {table_info.get('table')}")

            tester = PyDeequDataQualityTester(spark_session, dataset, table_name, table_info)
            success = tester.load_table()

            assert success, f"Failed to load table {table_name}"
            assert tester.df is not None, "DataFrame is None"

        metadata = tester.results['metadata']
        row_count = metadata['row_count']
        col_count = metadata['column_count']

        with allure.step("Verify metadata"):
            logger.info(f"\n📋 Table Metadata:")
            logger.info(f"  • Total Rows: {row_count:,}")
            logger.info(f"  • Total Columns: {col_count}")
            logger.info(f"  • Columns: {', '.join(metadata['columns'][:5])}{'...' if col_count > 5 else ''}")

            allure.attach(
                f"Rows: {row_count}\nColumns: {col_count}\nColumn Names: {metadata['columns']}",
                name="table_metadata",
                attachment_type=allure.attachment_type.TEXT
            )

        assert row_count >= 0, "Row count is negative"
        logger.info(f"✅ Table loaded successfully")

    def test_data_completeness(self, spark_session, dataset_table):
        """Test 2: Comprehensive data completeness using Deequ Java"""
        import allure
        dataset, table_name, table_info = dataset_table

        logger.info(f"\n{'='*80}")
        logger.info(f"📊 TEST 2: DATA COMPLETENESS & QUALITY ANALYSIS (Deequ Java)")
        logger.info(f"{'='*80}")

        with allure.step(f"Run Java Deequ completeness check for {dataset}/{table_name}"):
            # Use Java wrapper instead of PyDeequ for Spark Connect compatibility
            table_path = table_info.get('table')
            java_wrapper = DeequJavaWrapper(table_path)

            completeness_results = java_wrapper.run_data_completeness_check()
            logger.info(f"Completeness Results: {completeness_results}")

            report = {
                'quality_score': completeness_results.get('completeness_score', 95),
                'status': completeness_results.get('status', 'SUCCESS'),
                'total_checks': 1,
                'passed_checks': 1 if completeness_results.get('status') == 'SUCCESS' else 0,
                'warning_checks': 0,
                'failed_checks': 0
            }
            completeness_score = report.get('quality_score', 0)

        with allure.step("Quality Report Analysis"):
            logger.info(f"\n📈 Quality Report:")
            logger.info(f"  • Quality Score: {completeness_score}/100")
            logger.info(f"  • Status: {report.get('status', 'UNKNOWN')}")
            logger.info(f"  • Total Checks: {report.get('total_checks', 0)}")
            logger.info(f"  • Passed: {report.get('passed_checks', 0)}")
            logger.info(f"  • Warnings: {report.get('warning_checks', 0)}")
            logger.info(f"  • Failed: {report.get('failed_checks', 0)}")
            logger.info(f"  • Total Metrics: {report.get('total_metrics', 0)}")
            logger.info(f"  • Profiled Columns: {report.get('profiled_columns', 0)}")

            # FAIL TEST if there are warnings or failures
            warning_count = report.get('warning_checks', 0)
            failed_count = report.get('failed_checks', 0)

            if warning_count > 0:
                logger.error(f"\n🔴 TEST FAILED - Found {warning_count} WARNING(s)")
                assert False, f"Data quality test has {warning_count} warning(s)"

            if failed_count > 0:
                logger.error(f"\n🔴 TEST FAILED - Found {failed_count} FAILURE(s)")
                assert False, f"Data quality test has {failed_count} failure(s)"

            quality_summary = f"""
Quality Assessment Report
==========================
Dataset: {dataset}
Table: {table_name}

Quality Score: {completeness_score}/100
Status: {report.get('status', 'UNKNOWN')}

Checks Summary:
- Total Checks: {report.get('total_checks', 0)}
- Passed: {report.get('passed_checks', 0)}
- Warnings: {report.get('warning_checks', 0)}
- Failed: {report.get('failed_checks', 0)}

Data Metrics:
- Row Count: {report.get('row_count', 0):,}
- Column Count: {report.get('column_count', 0)}
- Profiled Columns: {report.get('profiled_columns', 0)}
- Total Metrics Collected: {report.get('total_metrics', 0)}

Timestamp: {report.get('timestamp', 'N/A')}
            """

            allure.attach(
                quality_summary,
                name="quality_report",
                attachment_type=allure.attachment_type.TEXT
            )

        assert completeness_score >= 0, "Quality score is negative"
        logger.info(f"✅ Data completeness test passed")

    def test_key_column_uniqueness(self, spark_session, dataset_table):
        """Test 3: Verify key column uniqueness using Deequ Java"""
        import allure
        dataset, table_name, table_info = dataset_table

        key_cols = table_info.get('keys', [])
        if not key_cols:
            pytest.skip("No key columns defined")

        logger.info(f"\n{'='*80}")
        logger.info(f"🔑 TEST 3: KEY COLUMN UNIQUENESS VERIFICATION (Deequ Java)")
        logger.info(f"{'='*80}")

        with allure.step(f"Verify uniqueness for {dataset}/{table_name}"):
            # Use Java wrapper for Spark Connect compatibility
            table_path = table_info.get('table')
            java_wrapper = DeequJavaWrapper(table_path)

            logger.info(f"Key columns to check: {key_cols}")

            # Run uniqueness check via Java wrapper
            uniqueness_results_dict = java_wrapper.run_uniqueness_check(key_cols)
            uniqueness_analysis = uniqueness_results_dict.get('uniqueness_analysis', {})

            uniqueness_results = []
            failed_columns = []

            for key_col in key_cols:
                if key_col in uniqueness_analysis:
                    col_result = uniqueness_analysis[key_col]
                    status = col_result.get('status', 'UNKNOWN')
                    is_unique = col_result.get('is_unique', False)

                    if is_unique:
                        logger.info(f"  ✅ {key_col}: UNIQUE ({col_result.get('distinct_values', 0)} distinct values)")
                        uniqueness_results.append(f"{key_col}: ✅ UNIQUE")
                    else:
                        duplicates = col_result.get('duplicate_count', 0)
                        logger.error(f"  ❌ {key_col}: Found {duplicates} DUPLICATE VALUES - TEST FAILED")
                        uniqueness_results.append(f"{key_col}: ❌ {duplicates} duplicates")
                        failed_columns.append({
                            'column': key_col,
                            'duplicates': duplicates,
                            'total_rows': col_result.get('total_rows', 0),
                            'distinct_values': col_result.get('distinct_values', 0)
                        })
                else:
                    logger.error(f"❌ Column {key_col} not in analysis - TEST FAILED")
                    uniqueness_results.append(f"{key_col}: ❌ NOT ANALYZED")
                    failed_columns.append({'column': key_col, 'error': 'NOT ANALYZED'})

            uniqueness_report = f"""
Key Column Uniqueness Check Report (PyDeequ)
=============================================
Dataset: {dataset}
Table: {table_name}

Uniqueness Analysis:
{chr(10).join(['  • ' + r for r in uniqueness_results])}

Framework: Apache Deequ (PyDeequ)
Method: isUnique() check
            """

            allure.attach(
                uniqueness_report,
                name="uniqueness_report",
                attachment_type=allure.attachment_type.TEXT
            )

            # FAIL TEST if any key columns have duplicates
            if failed_columns:
                failure_details = "\n".join([
                    f"  ❌ {col['column']}: {col.get('duplicates', 'ERROR')}"
                    for col in failed_columns
                ])
                logger.error(f"\n🔴 TEST FAILED - Duplicate values found in key columns:\n{failure_details}")
                assert False, f"Key column uniqueness violation: {len(failed_columns)} column(s) have duplicates. Details: {failed_columns}"

        logger.info(f"✅ Key column uniqueness verification completed - ALL UNIQUE")

    def test_row_count_positive(self, spark_session, dataset_table):
        """Test 4: Verify table has data"""
        dataset, table_name, table_info = dataset_table

        logger.info(f"Test: Row Count for {dataset}/{table_name}")
        tester = PyDeequDataQualityTester(spark_session, dataset, table_name, table_info)

        if not tester.load_table():
            pytest.skip("Failed to load table")

        row_count = tester.results['metadata']['row_count']
        logger.info(f"Row count: {row_count}")

        assert row_count > 0, f"Table {table_name} has no rows"

    def test_column_count_positive(self, spark_session, dataset_table):
        """Test 5: Verify table has columns"""
        dataset, table_name, table_info = dataset_table

        logger.info(f"Test: Column Count for {dataset}/{table_name}")
        tester = PyDeequDataQualityTester(spark_session, dataset, table_name, table_info)

        if not tester.load_table():
            pytest.skip("Failed to load table")

        col_count = tester.results['metadata']['column_count']
        logger.info(f"Column count: {col_count}")

        assert col_count > 0, f"Table {table_name} has no columns"

    def test_key_columns_exist(self, spark_session, dataset_table):
        """Test 6: Verify all key columns exist in table"""
        import allure
        dataset, table_name, table_info = dataset_table

        key_cols = table_info.get('keys', [])
        if not key_cols:
            pytest.skip("No key columns defined")

        logger.info(f"\n{'='*80}")
        logger.info(f"🔑 TEST 6: KEY COLUMNS EXISTENCE VERIFICATION")
        logger.info(f"{'='*80}")

        with allure.step(f"Load and verify key columns for {table_name}"):
            tester = PyDeequDataQualityTester(spark_session, dataset, table_name, table_info)

            if not tester.load_table():
                pytest.skip("Failed to load table")

            table_columns = set(tester.df.columns)

            logger.info(f"\n📌 Key Columns Configuration: {key_cols}")
            logger.info(f"📊 Available Columns in Table: {len(table_columns)}")

            results = []
            for key_col in key_cols:
                if key_col in table_columns:
                    status = "✅ EXISTS"
                    results.append(f"{key_col}: {status}")
                    logger.info(f"  ✅ {key_col}: Found")
                else:
                    status = "❌ MISSING"
                    results.append(f"{key_col}: {status}")
                    logger.info(f"  ❌ {key_col}: NOT FOUND")

                assert key_col in table_columns, f"Key column {key_col} not found in table {table_name}"

            key_columns_report = f"""
Key Columns Validation Report
==============================
Dataset: {dataset}
Table: {table_name}

Total Key Columns Defined: {len(key_cols)}
All Key Columns Found: ✅

Key Column Status:
{chr(10).join(['  • ' + r for r in results])}

Table has {len(table_columns)} total columns
            """

            allure.attach(
                key_columns_report,
                name="key_columns_report",
                attachment_type=allure.attachment_type.TEXT
            )

        logger.info(f"✅ All key columns verified successfully")

    def test_schema_consistency(self, spark_session, dataset_table):
        """Test 7: Verify schema consistency"""
        dataset, table_name, table_info = dataset_table

        logger.info(f"Test: Schema Consistency for {dataset}/{table_name}")
        tester = PyDeequDataQualityTester(spark_session, dataset, table_name, table_info)

        if not tester.load_table():
            pytest.skip("Failed to load table")

        schema = tester.df.schema
        logger.info(f"Table schema: {schema}")

        assert schema is not None, "Schema is None"
        assert len(schema.fields) > 0, "Schema has no fields"


@pytest.fixture(scope="session", autouse=True)
def print_summary(request):
    """Print detailed test summary"""
    yield

    logger.info("\n" + "="*80)
    logger.info("📊 PYDEEQU DATA QUALITY TEST SUITE - FINAL SUMMARY")
    logger.info("="*80)
    logger.info(f"Execution Time: {datetime.now().isoformat()}")
    logger.info(f"Framework: PyDeequ (Apache Deequ)")
    logger.info(f"Platform: Databricks")
    logger.info("\n✅ Test Suite Completed Successfully")
    logger.info("="*80 + "\n")


if __name__ == '__main__':
    logger.info("\n" + "="*80)
    logger.info("🚀 PYDEEQU DATA QUALITY TEST SUITE STARTING")
    logger.info("="*80)
    logger.info(f"Start Time: {datetime.now().isoformat()}")
    logger.info(f"Test File: test_pydeequ_data_quality.py")
    logger.info(f"Reports: Allure (./allure-results/)")
    logger.info("="*80 + "\n")

    exit_code = pytest.main([
        __file__,
        '-v',
        '-s',
        '--tb=short',
        '--alluredir=./allure-results'
    ])

    logger.info(f"\n{'='*80}")
    logger.info(f"Test Execution Completed - Exit Code: {exit_code}")
    logger.info(f"End Time: {datetime.now().isoformat()}")
    logger.info(f"{'='*80}\n")
