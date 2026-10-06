from pyspark.sql.functions import col, count, when, min, max , lower, trim ,expr, col, abs


# def full_schema_validation(df, base_df, column_name="Standardize Column Name"):
#
#     expected_cols = (
#         base_df[column_name]
#         .dropna()
#         .astype(str)
#         .str.strip()
#         .str.lower()
#         .tolist()
#     )
#
#     actual_cols = [c.strip().lower() for c in df.columns]
#
#     unexpected = [col for col in actual_cols if col not in expected_cols]
#     missing = [col for col in expected_cols if col not in actual_cols]
#
#     return {
#         "status": len(unexpected) == 0 and len(missing) == 0,
#         "unexpected_columns": unexpected,
#         "missing_columns": missing
#     }
def validate_expected_columns(
        df,
        base_df,
        column_name="Standardize Column Name"
):

    # ADP Columns
    expected_cols = set(
        base_df[column_name]
        .dropna()
        .astype(str)
        .str.strip()
        .str.replace("\n", " ", regex=False)
        .str.lower()
        .tolist()
    )

    # Dataset Columns
    actual_cols = set(
        col.strip()
        .replace("\n", " ")
        .lower()
        for col in df.columns
    )

    missing_columns = sorted(
        list(expected_cols - actual_cols)
    )

    unexpected_columns = sorted(
        list(actual_cols - expected_cols)
    )

    return {
        "status": (
                len(missing_columns) == 0 and
                len(unexpected_columns) == 0
        ),
        "missing_columns": missing_columns,
        "unexpected_columns": unexpected_columns,
        "expected_count": len(expected_cols),
        "actual_count": len(actual_cols),
        "missing_count": len(missing_columns),
        "unexpected_count": len(unexpected_columns)
    }
def get_null_counts(df):

    null_df = df.select([
        count(when(col(c).isNull(), c)).alias(c)
        for c in df.columns
    ])

    return null_df.collect()[0].asDict()


def get_min_max(df, columns):

    result = []

    for column in columns:
        if column not in df.columns:
            continue

        row = df.select(
            min(col(column)).alias("min"),
            max(col(column)).alias("max")
        ).collect()[0]

        result.append((column, row["min"], row["max"]))

    return result
#
# def check_composite_key_duplicates(df, columns):
#     """
#     Checks for duplicate records based on given composite key columns.
#
#     Returns:
#     -------
#     tuple:
#         dup_df   → DataFrame with duplicate keys
#         dup_count → total number of duplicate key groups
#     """
#
#     dup_df = (
#         df.groupBy(*columns)
#         .agg(count("*").alias("count"))
#         .filter(col("count") > 1)
#     )
#
#     dup_count = dup_df.count()
#
#     return dup_df, dup_count

def check_composite_key_duplicates(df, columns):
    """
    Checks for duplicate records based on given composite key columns.
    If 'is_current' column exists (case-insensitive),
    only rows where is_current is True are checked.

    Returns:
    -------
    tuple:
        dup_df    → DataFrame with duplicate keys
        dup_count → total number of duplicate key groups
    """

    # Find is_current column irrespective of case
    is_current_col = next(
        (c for c in df.columns if c.lower() == "is_current"),
        None
    )

    # Filter only current records if column exists
    if is_current_col:
        filtered_df = df.filter(col(is_current_col) == True)
        print("is_current_col", is_current_col)
    else:
        filtered_df = df

    # Find duplicate composite keys
    dup_df = (
        filtered_df.groupBy(*columns)
        .agg(count("*").alias("count"))
        .filter(col("count") > 1)
    )

    dup_count = dup_df.count()

    return dup_df, dup_count

def check_nulls_in_keys(df, keys):
    """
    Returns dataframe with null key records and count
    """
    condition = None

    for key in keys:
        if condition is None:
            condition = col(key).isNull()
        else:
            condition = condition | col(key).isNull()

    null_df = df.filter(condition)
    null_count = null_df.count()

    return null_df, null_count


def check_nulls_ignore_string_values(df, keys, ignore_values=None):
    """
    Generic function to:
    - Ignore specified string values (default: 'None')
    - Catch real NULL values in given keys
    """

    if not ignore_values:
        ignore_values = ["none"]  # default

    # Normalize ignore values
    ignore_values = [v.lower().strip() for v in ignore_values]

    # -------------------------------
    # Step 1: Ignore string values like "None", "NA"
    # -------------------------------
    filtered_df = df
    for k in keys:
        condition = ~lower(trim(col(k))).isin(ignore_values)
        filtered_df = filtered_df.filter(condition)

    # -------------------------------
    # Step 2: Catch real NULLs
    # -------------------------------
    null_condition = None
    for k in keys:
        cond = col(k).isNull()
        null_condition = cond if null_condition is None else (null_condition | cond)

    null_df = filtered_df.filter(null_condition)
    null_count = null_df.count()

    # -------------------------------
    # Metrics
    # -------------------------------
    total_count = df.count()
    filtered_count = filtered_df.count()
    ignored_count = total_count - filtered_count

    return {
        "null_df": null_df,
        "null_count": null_count,
        "ignored_count": ignored_count,
        "total_count": total_count,
        "checked_count": filtered_count
    }

def validate_formula_columns(df, base_df, config, keys):
    results = {}

    for target_col, rule in config.items():
        formula = rule["formula"]
        expected_col = f"expected_{target_col}"

        # ---------------------------
        # DATE handling
        # ---------------------------
        if "date" in formula.lower():
            df_calc = df.withColumn(expected_col, expr(formula))

            match_condition = (
                (col(target_col) == col(expected_col)) |
                (col(target_col).isNull() & col(expected_col).isNull())
            )

        # ---------------------------
        # NUMERIC handling
        # ---------------------------
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
                | ((col(target_col) == 0) & (abs(col(expected_col) - 1) < 0.0001))
            )

        mismatch_df = df_calc.filter(~match_condition)

        results[target_col] = mismatch_df

    return results




def check_composite_key_nulls(df, keys):
    """
    Checks if any of the composite key columns contain NULL values
    """

    # Build condition: any key column is NULL
    null_condition = None
    for k in keys:
        condition = col(k).isNull()
        null_condition = condition if null_condition is None else (null_condition | condition)

    # Filter rows where any key is NULL
    null_df = df.filter(null_condition)

    null_count = null_df.count()

    return null_df, null_count