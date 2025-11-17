import pandas as pd


def assert_sparkframes_equal(actual_df, expected_df, sort_keys):
    sort_keys = [sort_keys] if isinstance(sort_keys, str) else list(sort_keys)
    actual_pdf = actual_df.orderBy(sort_keys).toPandas().reset_index(drop=True)
    expected_pdf = expected_df.orderBy(sort_keys).toPandas().reset_index(drop=True)

    try:
        pd.testing.assert_frame_equal(actual_pdf, expected_pdf, check_dtype=False)
    except AssertionError as exc:
        comparison = pd.concat(
            [
                actual_pdf.assign(__source="actual"),
                expected_pdf.assign(__source="expected"),
            ],
            ignore_index=True,
        )
        comparison = comparison.sort_values(sort_keys + ["__source"]).reset_index(
            drop=True
        )
        raise AssertionError(
            "Spark DataFrames differ. Combined view (sorted):\n"
            f"{comparison.to_string(index=False)}"
        ) from exc
