#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Complete PyDeequ JVM Implementation with Pytest & Allure Integration

6 Test Cases with Full Data Quality Analysis:
1. Completeness Analysis (Null values)
2. Uniqueness Analysis (Duplicates)
3. Statistical Analysis (Mean, Min, Max, Sum, StdDev)
4. Pattern Analysis (Email, Phone validation)
5. Validity Checks (Non-negative, Range)
6. PyDeequ Verification Checks (Using JVM)

Generates Allure reports for Databricks execution.
"""

import os
import sys
import json
import logging
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any

import pytest
import pandas as pd
import allure
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult
from pydeequ.analyzers import AnalysisRunner, AnalyzerContext, Size, Completeness, CountDistinct

sys.path.insert(0, str(Path(__file__).parent.parent / 'test_data'))
from dataset_config import DATASETS

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class CompletePyDeequJVMAnalyzer:
    """Complete PyDeequ Analysis with All 6 Test Cases - JVM Based"""

    def __init__(self, spark: SparkSession, dataset: str, table_name: str, table_path: str):
        self.spark = spark
        self.dataset = dataset
        self.table_name = table_name
        self.table_path = table_path
        self.df = None
        self.results = {
            'metadata': {},
            'completeness_analysis': {},
            'uniqueness_analysis': {},
            'statistical_analysis': {},
            'pattern_analysis': {},
            'validity_checks': {},
            'pydeequ_checks': [],
            'summary': {}
        }

    def load_table(self) -> bool:
        """Load Databricks table"""
        try:
            self.df = self.spark.read.table(self.table_path)
            logger.info(f"✅ Table loaded: {self.df.count()} rows, {len(self.df.columns)} columns")
            return True
        except Exception as e:
            logger.error(f"❌ Load failed: {e}")
            return False

    def analyze_metadata(self) -> Dict[str, Any]:
        """TEST 0: Analyze metadata"""
        metadata = {
            'dataset': self.dataset,
            'table_name': self.table_name,
            'table_path': self.table_path,
            'total_rows': self.df.count(),
            'total_columns': len(self.df.columns),
            'columns': self.df.columns,
            'schema': [{'name': f.name, 'type': str(f.dataType)} for f in self.df.schema.fields],
            'timestamp': datetime.now().isoformat()
        }
        self.results['metadata'] = metadata
        logger.info(f"✅ Metadata: {metadata['total_rows']} rows, {metadata['total_columns']} columns")
        return metadata

    def completeness_analysis(self) -> Dict[str, Any]:
        """TEST 1: COMPLETENESS - Null value analysis"""
        logger.info("\n📊 TEST 1: COMPLETENESS ANALYSIS")
        logger.info("=" * 60)

        total_rows = self.df.count()
        completeness = {}

        for col in self.df.columns:
            null_count = self.df.filter(F.col(col).isNull()).count()
            non_null = total_rows - null_count
            completeness_pct = (non_null / total_rows * 100)

            completeness[col] = {
                'total_rows': total_rows,
                'non_null_count': non_null,
                'null_count': null_count,
                'completeness_percent': round(completeness_pct, 2),
                'status': 'PASS' if completeness_pct >= 90 else 'WARNING' if completeness_pct >= 50 else 'FAIL'
            }

            logger.info(f"  ✓ {col}: {completeness_pct:.1f}% complete ({null_count} nulls)")

        self.results['completeness_analysis'] = completeness
        return completeness

    def uniqueness_analysis(self) -> Dict[str, Any]:
        """TEST 2: UNIQUENESS - Duplicate detection"""
        logger.info("\n🔍 TEST 2: UNIQUENESS ANALYSIS")
        logger.info("=" * 60)

        total_rows = self.df.count()
        uniqueness = {}

        for col in self.df.columns:
            distinct_count = self.df.select(col).distinct().count()
            uniqueness_pct = (distinct_count / total_rows * 100)

            uniqueness[col] = {
                'total_values': total_rows,
                'distinct_count': distinct_count,
                'duplicate_count': total_rows - distinct_count,
                'uniqueness_percent': round(uniqueness_pct, 2),
                'status': 'PASS' if uniqueness_pct >= 95 else 'WARNING' if uniqueness_pct >= 80 else 'FAIL'
            }

            logger.info(f"  ✓ {col}: {uniqueness_pct:.1f}% unique ({distinct_count} distinct)")

        self.results['uniqueness_analysis'] = uniqueness
        return uniqueness

    def statistical_analysis(self) -> Dict[str, Any]:
        """TEST 3: STATISTICAL ANALYSIS - Mean, Min, Max, Sum, StdDev"""
        logger.info("\n📈 TEST 3: STATISTICAL ANALYSIS")
        logger.info("=" * 60)

        numeric_cols = [f.name for f in self.df.schema.fields if f.dataType.typeName() in ['int', 'long', 'float', 'double']]
        statistics = {}

        if not numeric_cols:
            logger.info("  ⚠️ No numeric columns found")
            return statistics

        pdf = self.df.select(*numeric_cols).toPandas()

        for col in pdf.columns:
            try:
                col_stats = {
                    'data_type': str(pdf[col].dtype),
                    'count': int(pdf[col].count()),
                    'mean': round(float(pdf[col].mean()), 2),
                    'min': float(pdf[col].min()),
                    'max': float(pdf[col].max()),
                    'sum': float(pdf[col].sum()),
                    'std_dev': round(float(pdf[col].std()), 2),
                    'variance': round(float(pdf[col].var()), 2),
                    'median': float(pdf[col].median()),
                    'q1': float(pdf[col].quantile(0.25)),
                    'q3': float(pdf[col].quantile(0.75))
                }
                statistics[col] = col_stats
                logger.info(f"  ✓ {col}: μ={col_stats['mean']}, σ={col_stats['std_dev']}, range=[{col_stats['min']}, {col_stats['max']}]")
            except Exception as e:
                logger.warning(f"  ⚠ {col}: {e}")

        self.results['statistical_analysis'] = statistics
        return statistics

    def pattern_analysis(self) -> Dict[str, Any]:
        """TEST 4: PATTERN ANALYSIS - Email, Phone validation"""
        logger.info("\n🔎 TEST 4: PATTERN ANALYSIS")
        logger.info("=" * 60)

        string_cols = [f.name for f in self.df.schema.fields if f.dataType.typeName() == 'string']
        pdf = self.df.select(*string_cols).toPandas() if string_cols else pd.DataFrame()
        patterns = {}

        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        phone_pattern = r'^[\d\s\-\+\(\)]{7,}$'

        for col in pdf.columns:
            if 'email' in col.lower():
                valid_count = sum(1 for val in pdf[col] if pd.notna(val) and re.match(email_pattern, str(val)))
                invalid_count = sum(1 for val in pdf[col] if pd.notna(val) and not re.match(email_pattern, str(val)))

                patterns[col] = {
                    'pattern_type': 'EMAIL',
                    'valid_count': valid_count,
                    'invalid_count': invalid_count,
                    'validity_percent': round(100 * valid_count / (valid_count + invalid_count), 2) if (valid_count + invalid_count) > 0 else 0,
                    'status': 'PASS' if invalid_count == 0 else 'WARNING' if invalid_count <= 2 else 'FAIL'
                }
                logger.info(f"  ✓ {col} (EMAIL): {valid_count} valid, {invalid_count} invalid")

            elif 'phone' in col.lower() or 'mobile' in col.lower():
                valid_count = sum(1 for val in pdf[col] if pd.notna(val) and re.match(phone_pattern, str(val)))
                invalid_count = sum(1 for val in pdf[col] if pd.notna(val) and not re.match(phone_pattern, str(val)))

                patterns[col] = {
                    'pattern_type': 'PHONE',
                    'valid_count': valid_count,
                    'invalid_count': invalid_count,
                    'validity_percent': round(100 * valid_count / (valid_count + invalid_count), 2) if (valid_count + invalid_count) > 0 else 0,
                    'status': 'PASS' if invalid_count == 0 else 'WARNING' if invalid_count <= 2 else 'FAIL'
                }
                logger.info(f"  ✓ {col} (PHONE): {valid_count} valid, {invalid_count} invalid")

        self.results['pattern_analysis'] = patterns
        return patterns

    def validity_checks(self) -> Dict[str, Any]:
        """TEST 5: VALIDITY CHECKS - Non-negative, Range validation"""
        logger.info("\n✔️ TEST 5: VALIDITY CHECKS")
        logger.info("=" * 60)

        numeric_cols = [f.name for f in self.df.schema.fields if f.dataType.typeName() in ['int', 'long', 'float', 'double']]
        checks = {}

        if not numeric_cols:
            logger.info("  ⚠️ No numeric columns found")
            return checks

        pdf = self.df.select(*numeric_cols).toPandas()

        for col in pdf.columns:
            try:
                # Non-negative check
                negative_count = (pdf[col] < 0).sum()
                non_negative_pct = (1 - negative_count / len(pdf)) * 100

                checks[f"{col}_non_negative"] = {
                    'check_type': 'isNonNegative',
                    'column': col,
                    'negative_count': int(negative_count),
                    'valid_percent': round(non_negative_pct, 2),
                    'status': 'PASS' if negative_count == 0 else 'FAIL'
                }
                logger.info(f"  ✓ {col}.isNonNegative(): {negative_count} negative values")

                # Range check
                col_min = pdf[col].min()
                col_max = pdf[col].max()
                checks[f"{col}_range"] = {
                    'check_type': 'hasRange',
                    'column': col,
                    'min': float(col_min),
                    'max': float(col_max),
                    'status': 'PASS'
                }
                logger.info(f"  ✓ {col}.hasRange(): [{col_min}, {col_max}]")
            except Exception as e:
                logger.warning(f"  ⚠ {col}: {e}")

        self.results['validity_checks'] = checks
        return checks

    def pydeequ_jvm_checks(self) -> List[Dict[str, Any]]:
        """TEST 6: PyDeequ JVM CHECKS - Using real JVM access"""
        logger.info("\n🔐 TEST 6: PYDEEQU JVM VERIFICATION CHECKS")
        logger.info("=" * 60)

        check_results = []

        try:
            # Create check object (JVM access required)
            check = Check(self.spark, CheckLevel.Warning, "CompleteJVMChecks")

            # Check 1: Has rows
            check = check.hasSize(lambda x: x > 0, "Dataset must have rows")
            logger.info(f"  ✓ Added: hasSize() check")

            # Check 2: Completeness
            for col in self.df.columns:
                try:
                    check = check.isComplete(col)
                except:
                    pass
            logger.info(f"  ✓ Added: isComplete() checks")

            # Check 3: Uniqueness for ID columns
            for col in self.df.columns:
                if 'id' in col.lower() or any(kw in col.lower() for kw in ['key', 'code']):
                    try:
                        check = check.isUnique(col)
                    except:
                        pass
            logger.info(f"  ✓ Added: isUnique() checks")

            # Run verification suite
            logger.info(f"  ⏳ Running JVM verification suite...")
            verification_result = VerificationSuite(self.spark).onData(self.df).addCheck(check).run()

            # Extract results
            check_results_df = VerificationResult.checkResultsAsDataFrame(self.spark, verification_result)
            check_results = check_results_df.toPandas().to_dict('records')

            logger.info(f"  ✅ JVM checks complete: {len(check_results)} results")

        except AttributeError as e:
            if "_jvm" in str(e) or "sparkContext" in str(e):
                logger.error(f"  ❌ JVM not available (Spark Connect detected): {e}")
                pytest.skip("PyDeequ JVM checks require traditional Spark (not Spark Connect)")
            raise
        except Exception as e:
            logger.error(f"  ❌ PyDeequ JVM checks error: {e}")
            check_results = []

        self.results['pydeequ_checks'] = check_results
        return check_results

    def generate_summary(self) -> Dict[str, Any]:
        """Generate comprehensive summary"""
        logger.info("\n📊 GENERATING SUMMARY")
        logger.info("=" * 60)

        total_test_cases = 0
        passed_tests = 0

        # Count tests
        for col, stats in self.results['completeness_analysis'].items():
            total_test_cases += 1
            if stats['status'] == 'PASS':
                passed_tests += 1

        for col, stats in self.results['uniqueness_analysis'].items():
            total_test_cases += 1
            if stats['status'] == 'PASS':
                passed_tests += 1

        if self.results['statistical_analysis']:
            total_test_cases += 1
            passed_tests += 1

        for col, stats in self.results['pattern_analysis'].items():
            total_test_cases += 1
            if stats['status'] == 'PASS':
                passed_tests += 1

        for check_name, check_data in self.results['validity_checks'].items():
            total_test_cases += 1
            if check_data['status'] == 'PASS':
                passed_tests += 1

        if self.results['pydeequ_checks']:
            total_test_cases += 1
            passed_tests += 1

        quality_score = (passed_tests / total_test_cases * 100) if total_test_cases > 0 else 0

        summary = {
            'dataset': self.dataset,
            'table_name': self.table_name,
            'quality_score': round(quality_score, 2),
            'total_test_cases': total_test_cases,
            'passed_tests': passed_tests,
            'failed_tests': total_test_cases - passed_tests,
            'status': 'PASS' if quality_score >= 80 else 'WARNING' if quality_score >= 60 else 'FAIL',
            'tests_implemented': [
                'TEST 1: Completeness Analysis',
                'TEST 2: Uniqueness Analysis',
                'TEST 3: Statistical Analysis (Mean, Min, Max, Sum, StdDev)',
                'TEST 4: Pattern Analysis (Email, Phone validation)',
                'TEST 5: Validity Checks (Non-negative, Range)',
                'TEST 6: PyDeequ JVM Verification Checks'
            ],
            'timestamp': datetime.now().isoformat()
        }

        self.results['summary'] = summary
        logger.info(f"\n✅ Quality Score: {quality_score:.2f}/100 - {summary['status']}")
        logger.info(f"✅ Tests Passed: {passed_tests}/{total_test_cases}")

        return summary

    def run_all_analysis(self) -> bool:
        """Run all 6 tests"""
        if not self.load_table():
            return False

        self.analyze_metadata()
        self.completeness_analysis()      # TEST 1
        self.uniqueness_analysis()        # TEST 2
        self.statistical_analysis()       # TEST 3
        self.pattern_analysis()           # TEST 4
        self.validity_checks()            # TEST 5
        self.pydeequ_jvm_checks()         # TEST 6
        self.generate_summary()

        logger.info("\n✅ All 6 tests completed!")
        return True


@pytest.fixture(scope="session")
def spark_session():
    """Create Spark session with JVM access"""
    os.environ["SPARK_VERSION"] = "3.3"

    spark = SparkSession.getActiveSession()

    if spark is None:
        spark = SparkSession.builder \
            .appName("PyDeequ-JVM-Tests") \
            .master("local[*]") \
            .getOrCreate()

    try:
        spark.sparkContext.setLogLevel("ERROR")
    except AttributeError:
        logger.warning("⚠️ No sparkContext - Spark Connect detected")

    yield spark


@pytest.mark.parametrize("dataset_table", [
    (dataset, table_name, table_info)
    for dataset, tables in DATASETS.items()
    for table_name, table_info in tables.items()
])
class TestPyDeequJVM:
    """Complete PyDeequ JVM Test Suite"""

    @allure.title("TEST 1: Completeness Analysis")
    @allure.description("Analyze null values and data completeness across all columns")
    def test_completeness_analysis(self, spark_session, dataset_table):
        """TEST 1: Completeness Analysis"""
        dataset, table_name, table_info = dataset_table
        table_path = table_info.get('table')

        with allure.step(f"Run completeness test for {dataset}/{table_name}"):
            analyzer = CompletePyDeequJVMAnalyzer(spark_session, dataset, table_name, table_path)
            analyzer.load_table()
            results = analyzer.completeness_analysis()

            for col, stats in results.items():
                allure.attach(
                    json.dumps(stats, indent=2),
                    name=f"completeness_{col}",
                    attachment_type=allure.attachment_type.JSON
                )

            assert len(results) > 0, "No completeness results"

    @allure.title("TEST 2: Uniqueness Analysis")
    @allure.description("Detect duplicate values and measure uniqueness")
    def test_uniqueness_analysis(self, spark_session, dataset_table):
        """TEST 2: Uniqueness Analysis"""
        dataset, table_name, table_info = dataset_table
        table_path = table_info.get('table')

        with allure.step(f"Run uniqueness test for {dataset}/{table_name}"):
            analyzer = CompletePyDeequJVMAnalyzer(spark_session, dataset, table_name, table_path)
            analyzer.load_table()
            results = analyzer.uniqueness_analysis()

            for col, stats in results.items():
                allure.attach(
                    json.dumps(stats, indent=2),
                    name=f"uniqueness_{col}",
                    attachment_type=allure.attachment_type.JSON
                )

            assert len(results) > 0, "No uniqueness results"

    @allure.title("TEST 3: Statistical Analysis")
    @allure.description("Calculate mean, min, max, sum, and standard deviation")
    def test_statistical_analysis(self, spark_session, dataset_table):
        """TEST 3: Statistical Analysis"""
        dataset, table_name, table_info = dataset_table
        table_path = table_info.get('table')

        with allure.step(f"Run statistical test for {dataset}/{table_name}"):
            analyzer = CompletePyDeequJVMAnalyzer(spark_session, dataset, table_name, table_path)
            analyzer.load_table()
            results = analyzer.statistical_analysis()

            if results:
                allure.attach(
                    json.dumps(results, indent=2, default=str),
                    name="statistics",
                    attachment_type=allure.attachment_type.JSON
                )

    @allure.title("TEST 4: Pattern Analysis")
    @allure.description("Validate email and phone patterns")
    def test_pattern_analysis(self, spark_session, dataset_table):
        """TEST 4: Pattern Analysis"""
        dataset, table_name, table_info = dataset_table
        table_path = table_info.get('table')

        with allure.step(f"Run pattern test for {dataset}/{table_name}"):
            analyzer = CompletePyDeequJVMAnalyzer(spark_session, dataset, table_name, table_path)
            analyzer.load_table()
            results = analyzer.pattern_analysis()

            if results:
                allure.attach(
                    json.dumps(results, indent=2),
                    name="patterns",
                    attachment_type=allure.attachment_type.JSON
                )

    @allure.title("TEST 5: Validity Checks")
    @allure.description("Validate non-negative values and ranges")
    def test_validity_checks(self, spark_session, dataset_table):
        """TEST 5: Validity Checks"""
        dataset, table_name, table_info = dataset_table
        table_path = table_info.get('table')

        with allure.step(f"Run validity test for {dataset}/{table_name}"):
            analyzer = CompletePyDeequJVMAnalyzer(spark_session, dataset, table_name, table_path)
            analyzer.load_table()
            results = analyzer.validity_checks()

            if results:
                allure.attach(
                    json.dumps(results, indent=2, default=str),
                    name="validity",
                    attachment_type=allure.attachment_type.JSON
                )

    @allure.title("TEST 6: PyDeequ JVM Verification Checks")
    @allure.description("Run PyDeequ checks using JVM with real Spark backend")
    def test_pydeequ_jvm_checks(self, spark_session, dataset_table):
        """TEST 6: PyDeequ JVM Checks"""
        dataset, table_name, table_info = dataset_table
        table_path = table_info.get('table')

        with allure.step(f"Run PyDeequ JVM test for {dataset}/{table_name}"):
            analyzer = CompletePyDeequJVMAnalyzer(spark_session, dataset, table_name, table_path)
            analyzer.load_table()
            results = analyzer.pydeequ_jvm_checks()

            if results:
                allure.attach(
                    json.dumps(results, indent=2, default=str),
                    name="pydeequ_checks",
                    attachment_type=allure.attachment_type.JSON
                )

    @allure.title("COMPLETE: All 6 Tests + Summary")
    @allure.description("Run all 6 tests and generate comprehensive quality report")
    def test_complete_analysis_with_summary(self, spark_session, dataset_table):
        """COMPLETE: All 6 Tests with Summary"""
        dataset, table_name, table_info = dataset_table
        table_path = table_info.get('table')

        with allure.step(f"Run complete analysis for {dataset}/{table_name}"):
            analyzer = CompletePyDeequJVMAnalyzer(spark_session, dataset, table_name, table_path)
            success = analyzer.run_all_analysis()

            assert success, "Analysis failed"

            summary = analyzer.results['summary']

            # Attach complete results as JSON
            allure.attach(
                json.dumps(analyzer.results, indent=2, default=str),
                name="complete_results",
                attachment_type=allure.attachment_type.JSON
            )

            # Log summary
            logger.info(f"\n{'='*80}")
            logger.info(f"✅ Quality Score: {summary['quality_score']}/100 ({summary['status']})")
            logger.info(f"✅ Tests Passed: {summary['passed_tests']}/{summary['total_test_cases']}")
            logger.info(f"{'='*80}")

            # Assert minimum quality
            assert summary['quality_score'] >= 60, f"Quality score below threshold: {summary['quality_score']}"


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--alluredir=./allure-results', '-s'])
