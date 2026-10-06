
from pyspark.sql.functions import expr, col

from pyspark.sql.functions import expr, col


def validate_derived_columns(df, config):
    results = {}

    for target_col, rule in config.items():
        formula = rule["formula"]
        expected_col = f"expected_{target_col}"

        df_calc = df.withColumn(expected_col, expr(formula))

        mismatch_df = df_calc.filter(
            ~(
                (col(target_col) == col(expected_col)) |
                (col(target_col).isNull() & col(expected_col).isNull())
            )
        )

        results[target_col] = {
            "count": mismatch_df.count(),
            "df": mismatch_df.select(target_col, expected_col).limit(20)
        }

    return results