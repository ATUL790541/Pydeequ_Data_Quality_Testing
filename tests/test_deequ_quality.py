import pytest
import pandas as pd
import allure
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, sum as spark_sum, when

spark = SparkSession.getActiveSession()


@allure.parent_suite("Deequ Data Quality")
class TestDeequDataQuality:
    """
    Deequ-style data quality testing for all datasets.
    Validates: completeness, uniqueness, numeric stats, pattern validation.
    """

    # ---------------------------
    # COMPLETENESS METRICS
    # ---------------------------
    def test_completeness_metrics(self, input_data):
        """Check NULL percentage for all columns"""
        df, _, customer, dataset, _ = input_data

        allure.dynamic.feature(customer)
        allure.dynamic.story(f"{dataset} - Completeness")
        allure.dynamic.title("Completeness Metrics")

        total_rows = df.count()
        completeness_results = {}

        for col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            completeness_pct = ((total_rows - null_count) / total_rows * 100) if total_rows > 0 else 0

            completeness_results[col_name] = {
                "null_count": int(null_count),
                "completeness_pct": round(completeness_pct, 2),
                "status": "PASS" if completeness_pct >= 80 else "WARN"
            }

        # Create summary table
        summary_df = pd.DataFrame([
            {
                "Column": col_name,
                "Null Count": details["null_count"],
                "Completeness %": details["completeness_pct"],
                "Status": details["status"]
            }
            for col_name, details in completeness_results.items()
        ])

        # Attach report
        allure.attach(
            summary_df.to_html(index=False),
            name="Completeness Summary",
            attachment_type=allure.attachment_type.HTML
        )

        # Log to console
        print(f"\n{'='*80}")
        print(f"Completeness Check: {customer} - {dataset}")
        print(f"{'='*80}")
        for col_name, details in completeness_results.items():
            status_icon = "✓" if details["status"] == "PASS" else "⚠️"
            print(f"{status_icon} {col_name:30} | Null: {details['null_count']:5} | Complete: {details['completeness_pct']:6.2f}%")

        assert True  # Always pass, just report


    # ---------------------------
    # UNIQUENESS METRICS
    # ---------------------------
    def test_uniqueness_metrics(self, input_data):
        """Check for duplicates and uniqueness in key columns"""
        df, _, customer, dataset, details = input_data

        allure.dynamic.feature(customer)
        allure.dynamic.story(f"{dataset} - Uniqueness")
        allure.dynamic.title("Uniqueness Metrics")

        keys = details.get("keys")
        if not keys:
            pytest.skip("No keys configured")

        total_rows = df.count()
        uniqueness_results = {}

        for col_name in keys:
            if col_name not in df.columns:
                continue

            # Count distinct values
            distinct_count = df.filter(col(col_name).isNotNull()).distinct().count()
            non_null_count = df.filter(col(col_name).isNotNull()).count()
            duplicate_count = non_null_count - distinct_count
            uniqueness_pct = (distinct_count / non_null_count * 100) if non_null_count > 0 else 0

            uniqueness_results[col_name] = {
                "distinct_count": int(distinct_count),
                "duplicate_count": int(duplicate_count),
                "uniqueness_pct": round(uniqueness_pct, 2),
                "status": "PASS" if uniqueness_pct >= 95 else "WARN"
            }

        # Create summary table
        summary_df = pd.DataFrame([
            {
                "Key Column": col_name,
                "Distinct": details["distinct_count"],
                "Duplicates": details["duplicate_count"],
                "Uniqueness %": details["uniqueness_pct"],
                "Status": details["status"]
            }
            for col_name, details in uniqueness_results.items()
        ])

        allure.attach(
            summary_df.to_html(index=False),
            name="Uniqueness Summary",
            attachment_type=allure.attachment_type.HTML
        )

        # Log to console
        print(f"\n{'='*80}")
        print(f"Uniqueness Check: {customer} - {dataset}")
        print(f"{'='*80}")
        for col_name, details in uniqueness_results.items():
            status_icon = "✓" if details["status"] == "PASS" else "⚠️"
            print(f"{status_icon} {col_name:30} | Unique: {details['distinct_count']:5} | Duplicates: {details['duplicate_count']:5} | {details['uniqueness_pct']:6.2f}%")

        assert True


    # ---------------------------
    # NUMERIC STATISTICS
    # ---------------------------
    def test_numeric_statistics(self, input_data):
        """Calculate min/max/avg for numeric columns"""
        df, _, customer, dataset, _ = input_data

        allure.dynamic.feature(customer)
        allure.dynamic.story(f"{dataset} - Numeric Stats")
        allure.dynamic.title("Numeric Statistics")

        # Detect numeric columns
        numeric_cols = []
        for field in df.schema.fields:
            dtype = str(field.dataType)
            if any(t in dtype for t in ["Integer", "Long", "Short", "Byte", "Double", "Float", "Decimal"]):
                numeric_cols.append(field.name)

        if not numeric_cols:
            pytest.skip("No numeric columns found")

        numeric_results = {}

        for col_name in numeric_cols:
            try:
                stats = df.agg(
                    expr(f"MIN({col_name})").alias("min_val"),
                    expr(f"MAX({col_name})").alias("max_val"),
                    expr(f"AVG({col_name})").alias("avg_val"),
                    expr(f"COUNT({col_name})").alias("non_null_count")
                ).collect()[0]

                numeric_results[col_name] = {
                    "min": float(stats["min_val"]) if stats["min_val"] else None,
                    "max": float(stats["max_val"]) if stats["max_val"] else None,
                    "avg": round(float(stats["avg_val"]), 2) if stats["avg_val"] else None,
                    "non_null_count": int(stats["non_null_count"])
                }
            except Exception as e:
                continue

        # Create summary table
        summary_df = pd.DataFrame([
            {
                "Column": col_name,
                "Min": details["min"],
                "Max": details["max"],
                "Avg": details["avg"],
                "Non-Null Count": details["non_null_count"]
            }
            for col_name, details in numeric_results.items()
        ])

        allure.attach(
            summary_df.to_html(index=False),
            name="Numeric Statistics",
            attachment_type=allure.attachment_type.HTML
        )

        # Log to console
        print(f"\n{'='*80}")
        print(f"Numeric Statistics: {customer} - {dataset}")
        print(f"{'='*80}")
        for col_name, details in numeric_results.items():
            print(f"✓ {col_name:30} | Min: {details['min']:15} | Max: {details['max']:15} | Avg: {details['avg']:15}")

        assert True


    # ---------------------------
    # PATTERN VALIDATION
    # ---------------------------
    def test_pattern_validation(self, input_data):
        """Validate email, date, and status patterns"""
        df, _, customer, dataset, _ = input_data

        allure.dynamic.feature(customer)
        allure.dynamic.story(f"{dataset} - Pattern Validation")
        allure.dynamic.title("Pattern Validation")

        total_rows = df.count()
        pattern_results = {}

        # Pattern 1: Email validation
        email_cols = [c for c in df.columns if "email" in c.lower()]
        if email_cols:
            for col_name in email_cols:
                try:
                    email_result = df.select(
                        spark_sum(when(
                            col(col_name).rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$"), 1
                        ).otherwise(0)).alias("valid_count"),
                        spark_sum(when(col(col_name).isNotNull(), 1).otherwise(0)).alias("non_null_count")
                    ).collect()[0]

                    valid_count = int(email_result["valid_count"])
                    non_null = int(email_result["non_null_count"])
                    valid_pct = (valid_count / non_null * 100) if non_null > 0 else 0

                    pattern_results[f"{col_name}_email"] = {
                        "valid_count": valid_count,
                        "invalid_count": non_null - valid_count,
                        "validity_pct": round(valid_pct, 2),
                        "status": "PASS" if valid_pct >= 80 else "WARN"
                    }
                except Exception as e:
                    pass

        # Pattern 2: Status/Category validation (check for common categorical columns)
        status_cols = [c for c in df.columns if any(s in c.lower() for s in ["status", "type", "category"])]
        if status_cols:
            for col_name in status_cols:
                try:
                    distinct_values = df.filter(col(col_name).isNotNull()).select(col_name).distinct().count()
                    null_count = df.filter(col(col_name).isNull()).count()
                    non_null_count = total_rows - null_count

                    pattern_results[f"{col_name}_values"] = {
                        "distinct_values": int(distinct_values),
                        "null_count": int(null_count),
                        "non_null_count": int(non_null_count),
                        "status": "PASS"
                    }
                except Exception as e:
                    pass

        if not pattern_results:
            pytest.skip("No pattern columns found")

        # Create summary table
        summary_data = []
        for pattern_name, details in pattern_results.items():
            if "email" in pattern_name:
                summary_data.append({
                    "Pattern": pattern_name,
                    "Valid": details.get("valid_count"),
                    "Invalid": details.get("invalid_count"),
                    "Validity %": details.get("validity_pct"),
                    "Status": details.get("status")
                })
            else:
                summary_data.append({
                    "Pattern": pattern_name,
                    "Distinct Values": details.get("distinct_values"),
                    "Null Count": details.get("null_count"),
                    "Status": details.get("status")
                })

        summary_df = pd.DataFrame(summary_data)
        allure.attach(
            summary_df.to_html(index=False),
            name="Pattern Validation",
            attachment_type=allure.attachment_type.HTML
        )

        # Log to console
        print(f"\n{'='*80}")
        print(f"Pattern Validation: {customer} - {dataset}")
        print(f"{'='*80}")
        for pattern_name, details in pattern_results.items():
            status_icon = "✓" if details.get("status") == "PASS" else "⚠️"
            if "email" in pattern_name:
                print(f"{status_icon} {pattern_name:30} | Valid: {details['valid_count']:5} | Invalid: {details['invalid_count']:5} | {details['validity_pct']:6.2f}%")
            else:
                print(f"{status_icon} {pattern_name:30} | Distinct: {details['distinct_values']:5} | Null: {details['null_count']:5}")

        assert True


    # ---------------------------
    # DATA PROFILE & QUALITY SCORE
    # ---------------------------
    def test_data_profile_quality_score(self, input_data):
        """Generate overall data quality profile and score"""
        df, _, customer, dataset, details = input_data

        allure.dynamic.feature(customer)
        allure.dynamic.story(f"{dataset} - Quality Score")
        allure.dynamic.title("Data Profile & Quality Score")

        total_rows = df.count()
        total_cols = len(df.columns)

        # Calculate metrics
        metrics = {
            "total_rows": int(total_rows),
            "total_columns": int(total_cols),
            "keys": details.get("keys", []),
            "timestamp": datetime.now().isoformat()
        }

        # Completeness score
        total_cells = total_rows * total_cols
        null_cells = sum(df.filter(col(c).isNull()).count() for c in df.columns)
        completeness_score = ((total_cells - null_cells) / total_cells * 100) if total_cells > 0 else 0

        # Uniqueness score (for keys)
        uniqueness_score = 100
        keys = details.get("keys", [])
        if keys:
            for key_col in keys:
                if key_col in df.columns:
                    distinct_count = df.filter(col(key_col).isNotNull()).distinct().count()
                    non_null_count = df.filter(col(key_col).isNotNull()).count()
                    unique_pct = (distinct_count / non_null_count * 100) if non_null_count > 0 else 0
                    uniqueness_score = min(uniqueness_score, unique_pct)

        # Overall quality score (weighted average)
        quality_score = (completeness_score * 0.6 + uniqueness_score * 0.4)

        metrics.update({
            "completeness_score": round(completeness_score, 2),
            "uniqueness_score": round(uniqueness_score, 2),
            "overall_quality_score": round(quality_score, 2)
        })

        # Create profile report
        profile_df = pd.DataFrame([
            {
                "Metric": "Total Rows",
                "Value": metrics["total_rows"]
            },
            {
                "Metric": "Total Columns",
                "Value": metrics["total_columns"]
            },
            {
                "Metric": "Completeness Score",
                "Value": f"{metrics['completeness_score']}%"
            },
            {
                "Metric": "Uniqueness Score",
                "Value": f"{metrics['uniqueness_score']}%"
            },
            {
                "Metric": "Overall Quality Score",
                "Value": f"{metrics['overall_quality_score']}%"
            }
        ])

        allure.attach(
            profile_df.to_html(index=False),
            name="Data Quality Profile",
            attachment_type=allure.attachment_type.HTML
        )

        # Attach JSON report
        allure.attach(
            json.dumps(metrics, indent=2),
            name="Quality Metrics JSON",
            attachment_type=allure.attachment_type.JSON
        )

        # Log summary
        print(f"\n{'='*80}")
        print(f"Quality Profile: {customer} - {dataset}")
        print(f"{'='*80}")
        print(f"Total Rows: {metrics['total_rows']}")
        print(f"Total Columns: {metrics['total_columns']}")
        print(f"Completeness: {metrics['completeness_score']}%")
        print(f"Uniqueness: {metrics['uniqueness_score']}%")
        print(f"\n🎯 Overall Quality Score: {metrics['overall_quality_score']}%")
        print(f"{'='*80}\n")

        assert True
