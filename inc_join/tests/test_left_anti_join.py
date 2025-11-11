from datetime import date, datetime
from decimal import Decimal

import pandas as pd
import pytest
from pyspark.sql import Row, SparkSession

JOIN_RESULT_SCHEMA = (
    "TrxId INT, RecDate DATE, RecDate_A DATE, RecDate_B DATE, DiffArrivalTime INT, JoinType STRING"
)

JOIN_RESULT_SCHEMA_WITH_WAITING = (
    "TrxId INT, RecDate DATE, RecDate_A DATE, RecDate_B DATE, DiffArrivalTime INT, WaitingTime INT, JoinType STRING"
)



from src import inc_join
from src.inc_join import IncJoinSettings


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    import logging
    import os

    logger = logging.getLogger(__name__)

    # Check if an external Spark master URL is provided via environment variable
    # Set SPARK_MASTER_URL to connect to an external Spark server
    # Examples:
    #   - Spark Standalone: "spark://hostname:7077"
    #   - Spark Connect: "sc://hostname:15002"
    #   - Local (default): "local[1]"
    master_url = os.environ.get("SPARK_MASTER_URL", "local[1]")

    # Check if we should use Spark Connect (client mode)
    use_spark_connect = os.environ.get("SPARK_CONNECT_URL")

    if use_spark_connect:
        logger.info(f"Connecting to Spark Connect server: {use_spark_connect}")
    elif master_url != "local[1]":
        logger.info(f"Connecting to Spark master: {master_url}")
    else:
        logger.info("Using local Spark mode (local[1])")

    if use_spark_connect:
        # Spark Connect mode - connect to remote Spark server
        # Note: Requires Spark 3.4+ for .remote() method
        try:
            session = (
                SparkSession.builder.remote(use_spark_connect)
                .appName("inc_join_tests")
                .config("spark.ui.showConsoleProgress", "false")
                .getOrCreate()
            )
        except AttributeError:
            raise RuntimeError(
                "Spark Connect requires Spark 3.4+. "
                "Your Spark version may not support .remote(). "
                "Try using SPARK_MASTER_URL instead, or upgrade Spark."
            )
        # Don't stop external Spark session
        session.conf.set("spark.sql.session.timeZone", "Europe/Amsterdam")
        yield session
        # Note: We don't call session.stop() for external Spark Connect sessions
    else:
        # Traditional Spark mode
        session = (
            SparkSession.builder.master(master_url)
            .appName("inc_join_tests")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate()
        )
        session.conf.set("spark.sql.session.timeZone", "Europe/Amsterdam")
        yield session
        # Only stop if it's a local session we created
        if master_url.startswith("local"):
            session.stop()
        # For external masters, we might want to keep the session alive
        # Uncomment the next line if you want to stop external sessions too:
        # session.stop()


def create_example_data(spark: SparkSession):
    schema = "TrxDT Timestamp, CreditDebit String, AmountEuro Decimal(12,2), AccountName String, TrxId Integer, RecDate Date"
    fin_data = [
        Row(TrxDT=datetime(2025, 3, 6, 20, 45, 19), CreditDebit="Credit", AmountEuro=Decimal(700.30), AccountName="Mrs. Zsa Zsa", TrxId=1, RecDate=date(2025, 3, 6)),
        Row(TrxDT=datetime(2025, 3, 6, 12, 22, 1), CreditDebit="Debit", AmountEuro=Decimal(200.00), AccountName="Mrs. Zsa Zsa", TrxId=2, RecDate=date(2025, 3, 6)),
        Row(TrxDT=datetime(2025, 3, 6, 20, 59, 0), CreditDebit="Debit", AmountEuro=Decimal(1110.20), AccountName="Mrs. Zsa Zsa", TrxId=3, RecDate=date(2025, 3, 6)),
        Row(TrxDT=datetime(2025, 3, 6, 23, 50, 0), CreditDebit="Credit", AmountEuro=Decimal(50.00), AccountName="Mrs. Zsa Zsa", TrxId=4, RecDate=date(2025, 3, 7)),
        Row(TrxDT=datetime(2025, 3, 6, 8, 0, 0), CreditDebit="Credit", AmountEuro=Decimal(1500.00), AccountName="Mr. X", TrxId=5, RecDate=date(2025, 3, 7)),
        Row(TrxDT=datetime(2025, 3, 7, 14, 45, 0), CreditDebit="Debit", AmountEuro=Decimal(300.25), AccountName="Mr. X", TrxId=6, RecDate=date(2025, 3, 7)),
        Row(TrxDT=datetime(2025, 3, 10, 9, 0, 0), CreditDebit="Credit", AmountEuro=Decimal(99.99), AccountName="Mr. X", TrxId=7, RecDate=date(2025, 3, 8)),
    ]
    df_a = spark.createDataFrame(fin_data, schema=schema)

    sepa_schema = "TrxId Integer, CountryCode String, RecDate Date"
    sepa_data = [
        Row(TrxId=1, CountryCode="NL", RecDate=date(2025, 3, 5)),
        Row(TrxId=2, CountryCode="NL", RecDate=date(2025, 3, 4)),
        Row(TrxId=3, CountryCode="NL", RecDate=date(2025, 3, 6)),
        Row(TrxId=4, CountryCode="UK", RecDate=date(2025, 3, 7)),
        Row(TrxId=5, CountryCode="NL", RecDate=date(2025, 3, 12)),
        Row(TrxId=6, CountryCode="NL", RecDate=date(2025, 3, 18)),
        Row(TrxId=7, CountryCode="DE", RecDate=date(2025, 3, 6)),
    ]
    df_b = spark.createDataFrame(sepa_data, schema=sepa_schema)

    # simplify the example by dropping some columns
    df_a = df_a.drop("TrxDT", "AccountName", "CreditDebit", "AmountEuro")
    df_b = df_b.drop("CountryCode")
    return df_a, df_b


def print_jointype_legend() -> None:
    print("JoinType legend: same_time, a_late, b_late, a_timed_out, a_waiting")


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


# Test look back
# When look back is 1 Trx 1 should be looked back, but 2 and 7 should not.
# Waiting time is 0, so 5 and 6 should not be in the output.
# Of course 3 and 4 should be in the output, because they are on time.
def test_left_anti_join_look_back_eq_1(spark: SparkSession):
    df_a, df_b = create_example_data(spark)
    settings = IncJoinSettings(
        inc_col_name="RecDate",
        include_waiting=False,
        enforce_sliding_join_window=True,
    )
    joined = inc_join(
        df_a,
        df_b,
        how="left_anti",
        join_cols="TrxId",
        look_back_time=1,
        max_waiting_time=0,
        other_settings=settings,
        output_window_start_dt=datetime(2025, 3, 1),
        output_window_end_dt=datetime(2025, 3, 30),
    )
    joined = joined.orderBy("TrxId")
    joined.show(truncate=True)
    print_jointype_legend()

    expected = spark.createDataFrame(
        [
            Row(TrxId=1, RecDate=date(2025, 3, 6), RecDate_A=date(2025, 3, 6), RecDate_B=date(2025, 3, 5), DiffArrivalTime=-1, JoinType="a_late"),
            Row(TrxId=2, RecDate=date(2025, 3, 6), RecDate_A=date(2025, 3, 6), RecDate_B=None, DiffArrivalTime=None, JoinType="a_timed_out"),
            Row(TrxId=3, RecDate=date(2025, 3, 6), RecDate_A=date(2025, 3, 6), RecDate_B=date(2025, 3, 6), DiffArrivalTime=0, JoinType="same_time"),
            Row(TrxId=4, RecDate=date(2025, 3, 7), RecDate_A=date(2025, 3, 7), RecDate_B=date(2025, 3, 7), DiffArrivalTime=0, JoinType="same_time"),
            Row(TrxId=5, RecDate=date(2025, 3, 7), RecDate_A=date(2025, 3, 7), RecDate_B=None, DiffArrivalTime=None, JoinType="a_timed_out"),
            Row(TrxId=6, RecDate=date(2025, 3, 7), RecDate_A=date(2025, 3, 7), RecDate_B=None, DiffArrivalTime=None, JoinType="a_timed_out"),
            Row(TrxId=7, RecDate=date(2025, 3, 8), RecDate_A=date(2025, 3, 8), RecDate_B=None, DiffArrivalTime=None, JoinType="a_timed_out"),
        ],
        schema=JOIN_RESULT_SCHEMA,
    )
    actual = joined.select(expected.columns).orderBy("TrxId")
    assert_sparkframes_equal(actual, expected, sort_keys="TrxId")


def test_left_anti_join_no_enforce_sliding_window(spark: SparkSession):
    """When we set enforce_sliding_join_window=False, the size of the output window determines our matching
    success. E.g. a large output window gives better matching. Note that we do extend the output window
    with -look_back_time and +max_waiting_time.
    """
    df_a, df_b = create_example_data(spark)

    settings = IncJoinSettings(
        inc_col_name="RecDate",
        include_waiting=False,
        enforce_sliding_join_window=False,
    )
    actual = inc_join(
        df_a,
        df_b,
        how="left_anti",
        join_cols="TrxId",
        look_back_time=3,
        max_waiting_time=5,
        other_settings=settings,
        output_window_start_dt=datetime(2025, 3, 1),
        output_window_end_dt=datetime(2025, 3, 30),
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)
    print_jointype_legend()

    expected = spark.createDataFrame(
        [
            Row(TrxId=1, RecDate=date(2025, 3, 6), RecDate_A=date(2025, 3, 6), RecDate_B=date(2025, 3, 5), DiffArrivalTime=-1, JoinType="a_late"),
            Row(TrxId=2, RecDate=date(2025, 3, 6), RecDate_A=date(2025, 3, 6), RecDate_B=date(2025, 3, 4), DiffArrivalTime=-2, JoinType="a_late"),
            Row(TrxId=3, RecDate=date(2025, 3, 6), RecDate_A=date(2025, 3, 6), RecDate_B=date(2025, 3, 6), DiffArrivalTime=0, JoinType="same_time"),
            Row(TrxId=4, RecDate=date(2025, 3, 7), RecDate_A=date(2025, 3, 7), RecDate_B=date(2025, 3, 7), DiffArrivalTime=0, JoinType="same_time"),
            Row(TrxId=5, RecDate=date(2025, 3, 12), RecDate_A=date(2025, 3, 7), RecDate_B=date(2025, 3, 12), DiffArrivalTime=5, JoinType="b_late"),
            Row(TrxId=6, RecDate=date(2025, 3, 18), RecDate_A=date(2025, 3, 7), RecDate_B=date(2025, 3, 18), DiffArrivalTime=11, JoinType="b_late"),
            Row(TrxId=7, RecDate=date(2025, 3, 8), RecDate_A=date(2025, 3, 8), RecDate_B=date(2025, 3, 6), DiffArrivalTime=-2, JoinType="a_late"),
        ],
        schema=JOIN_RESULT_SCHEMA,
    )
    actual = actual.select(expected.columns).orderBy("TrxId")
    assert_sparkframes_equal(actual, expected, sort_keys="TrxId")


def test_left_anti_join_timed_out_rows(spark: SparkSession):
    df_a, df_b = create_example_data(spark)

    settings = IncJoinSettings(
        inc_col_name="RecDate",
        include_waiting=False,
        enforce_sliding_join_window=True,
    )
    actual = inc_join(
        df_a,
        df_b,
        how="left_anti",
        join_cols="TrxId",
        look_back_time=3,
        max_waiting_time=5,
        other_settings=settings,
        output_window_start_dt=datetime(2025, 3, 1),
        output_window_end_dt=datetime(2025, 3, 20),
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)
    print_jointype_legend()

    expected = spark.createDataFrame(
        [
            Row(TrxId=1, RecDate=date(2025, 3, 6), RecDate_A=date(2025, 3, 6), RecDate_B=date(2025, 3, 5), DiffArrivalTime=-1, JoinType="a_late"),
            Row(TrxId=2, RecDate=date(2025, 3, 6), RecDate_A=date(2025, 3, 6), RecDate_B=date(2025, 3, 4), DiffArrivalTime=-2, JoinType="a_late"),
            Row(TrxId=3, RecDate=date(2025, 3, 6), RecDate_A=date(2025, 3, 6), RecDate_B=date(2025, 3, 6), DiffArrivalTime=0, JoinType="same_time"),
            Row(TrxId=4, RecDate=date(2025, 3, 7), RecDate_A=date(2025, 3, 7), RecDate_B=date(2025, 3, 7), DiffArrivalTime=0, JoinType="same_time"),
            Row(TrxId=5, RecDate=date(2025, 3, 12), RecDate_A=date(2025, 3, 7), RecDate_B=date(2025, 3, 12), DiffArrivalTime=5, JoinType="b_late"),
            Row(TrxId=6, RecDate=date(2025, 3, 12), RecDate_A=date(2025, 3, 7), RecDate_B=None, DiffArrivalTime=None, JoinType="a_timed_out"),
            Row(TrxId=7, RecDate=date(2025, 3, 8), RecDate_A=date(2025, 3, 8), RecDate_B=date(2025, 3, 6), DiffArrivalTime=-2, JoinType="a_late"),
        ],
        schema=JOIN_RESULT_SCHEMA,
    )
    actual = actual.select(expected.columns).orderBy("TrxId")
    assert_sparkframes_equal(actual, expected, sort_keys="TrxId")


def test_left_anti_join_small_output_window(spark: SparkSession):
    df_a, df_b = create_example_data(spark)

    settings = IncJoinSettings(
        inc_col_name="RecDate",
        include_waiting=False,
        enforce_sliding_join_window=True,
    )
    actual = inc_join(
        df_a,
        df_b,
        how="left_anti",
        join_cols="TrxId",
        look_back_time=3,
        max_waiting_time=5,
        other_settings=settings,
        output_window_start_dt=datetime(2025, 3, 1),
        output_window_end_dt=datetime(2025, 3, 9),
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)
    print_jointype_legend()

    expected = spark.createDataFrame(
        [
            Row(TrxId=1, RecDate=date(2025, 3, 6), RecDate_A=date(2025, 3, 6), RecDate_B=date(2025, 3, 5), DiffArrivalTime=-1, JoinType="a_late"),
            Row(TrxId=2, RecDate=date(2025, 3, 6), RecDate_A=date(2025, 3, 6), RecDate_B=date(2025, 3, 4), DiffArrivalTime=-2, JoinType="a_late"),
            Row(TrxId=3, RecDate=date(2025, 3, 6), RecDate_A=date(2025, 3, 6), RecDate_B=date(2025, 3, 6), DiffArrivalTime=0, JoinType="same_time"),
            Row(TrxId=4, RecDate=date(2025, 3, 7), RecDate_A=date(2025, 3, 7), RecDate_B=date(2025, 3, 7), DiffArrivalTime=0, JoinType="same_time"),
            Row(TrxId=7, RecDate=date(2025, 3, 8), RecDate_A=date(2025, 3, 8), RecDate_B=date(2025, 3, 6), DiffArrivalTime=-2, JoinType="a_late"),
        ],
        schema=JOIN_RESULT_SCHEMA,
    )
    actual = actual.select(expected.columns).orderBy("TrxId")
    assert_sparkframes_equal(actual, expected, sort_keys="TrxId")


def test_left_anti_join_march_6(spark: SparkSession):
    df_a, df_b = create_example_data(spark)

    settings = IncJoinSettings(
        inc_col_name="RecDate",
        include_waiting=False,
        enforce_sliding_join_window=True,
    )
    actual = inc_join(
        df_a,
        df_b,
        how="left_anti",
        join_cols="TrxId",
        look_back_time=3,
        max_waiting_time=5,
        other_settings=settings,
        output_window_start_dt=datetime(2025, 3, 6),
        output_window_end_dt=datetime(2025, 3, 6),
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)
    print_jointype_legend()

    expected = spark.createDataFrame(
        [
            Row(TrxId=1, RecDate=date(2025, 3, 6), RecDate_A=date(2025, 3, 6), RecDate_B=date(2025, 3, 5), DiffArrivalTime=-1, JoinType="a_late"),
            Row(TrxId=2, RecDate=date(2025, 3, 6), RecDate_A=date(2025, 3, 6), RecDate_B=date(2025, 3, 4), DiffArrivalTime=-2, JoinType="a_late"),
            Row(TrxId=3, RecDate=date(2025, 3, 6), RecDate_A=date(2025, 3, 6), RecDate_B=date(2025, 3, 6), DiffArrivalTime=0, JoinType="same_time"),
        ],
        schema=JOIN_RESULT_SCHEMA,
    )
    actual = actual.select(expected.columns).orderBy("TrxId")
    assert_sparkframes_equal(actual, expected, sort_keys="TrxId")


def test_left_anti_join_march_7(spark: SparkSession):
    df_a, df_b = create_example_data(spark)

    settings = IncJoinSettings(
        inc_col_name="RecDate",
        include_waiting=False,
        enforce_sliding_join_window=True,
    )
    actual = inc_join(
        df_a,
        df_b,
        how="left_anti",
        join_cols="TrxId",
        look_back_time=3,
        max_waiting_time=5,
        other_settings=settings,
        output_window_start_dt=datetime(2025, 3, 7),
        output_window_end_dt=datetime(2025, 3, 7),
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)
    print_jointype_legend()

    expected = spark.createDataFrame(
        [
            Row(TrxId=4, RecDate=date(2025, 3, 7), RecDate_A=date(2025, 3, 7), RecDate_B=date(2025, 3, 7), DiffArrivalTime=0, JoinType="same_time"),
        ],
        schema=JOIN_RESULT_SCHEMA,
    )
    actual = actual.select(expected.columns).orderBy("TrxId")
    assert_sparkframes_equal(actual, expected, sort_keys="TrxId")


def test_left_anti_join_march_8(spark: SparkSession):
    df_a, df_b = create_example_data(spark)

    settings = IncJoinSettings(
        inc_col_name="RecDate",
        include_waiting=False,
        enforce_sliding_join_window=True,
    )
    actual = inc_join(
        df_a,
        df_b,
        how="left_anti",
        join_cols="TrxId",
        look_back_time=3,
        max_waiting_time=5,
        other_settings=settings,
        output_window_start_dt=datetime(2025, 3, 8),
        output_window_end_dt=datetime(2025, 3, 8),
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)
    print_jointype_legend()

    expected = spark.createDataFrame(
        [
            Row(TrxId=7, RecDate=date(2025, 3, 8), RecDate_A=date(2025, 3, 8), RecDate_B=date(2025, 3, 6), DiffArrivalTime=-2, JoinType="a_late"),
        ],
        schema=JOIN_RESULT_SCHEMA,
    )
    actual = actual.select(expected.columns).orderBy("TrxId")
    assert_sparkframes_equal(actual, expected, sort_keys="TrxId")


def test_left_anti_join_march_9(spark: SparkSession):
    df_a, df_b = create_example_data(spark)

    settings = IncJoinSettings(
        inc_col_name="RecDate",
        include_waiting=False,
        enforce_sliding_join_window=True,
    )
    actual = inc_join(
        df_a,
        df_b,
        how="left_anti",
        join_cols="TrxId",
        look_back_time=3,
        max_waiting_time=5,
        other_settings=settings,
        output_window_start_dt=datetime(2025, 3, 9),
        output_window_end_dt=datetime(2025, 3, 9),
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)
    print_jointype_legend()

    expected = spark.createDataFrame([], schema=actual.schema)
    actual = actual.select(expected.columns).orderBy("TrxId")
    assert_sparkframes_equal(actual, expected, sort_keys="TrxId")


def test_left_anti_join_march_10(spark: SparkSession):
    df_a, df_b = create_example_data(spark)

    settings = IncJoinSettings(
        inc_col_name="RecDate",
        include_waiting=False,
        enforce_sliding_join_window=True,
    )
    actual = inc_join(
        df_a,
        df_b,
        how="left_anti",
        join_cols="TrxId",
        look_back_time=3,
        max_waiting_time=5,
        other_settings=settings,
        output_window_start_dt=datetime(2025, 3, 10),
        output_window_end_dt=datetime(2025, 3, 10),
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)
    print_jointype_legend()

    expected = spark.createDataFrame([], schema=actual.schema)
    actual = actual.select(expected.columns).orderBy("TrxId")
    assert_sparkframes_equal(actual, expected, sort_keys="TrxId")


def test_left_anti_join_march_11(spark: SparkSession):
    df_a, df_b = create_example_data(spark)

    settings = IncJoinSettings(
        inc_col_name="RecDate",
        include_waiting=False,
        enforce_sliding_join_window=True,
    )
    actual = inc_join(
        df_a,
        df_b,
        how="left_anti",
        join_cols="TrxId",
        look_back_time=3,
        max_waiting_time=5,
        other_settings=settings,
        output_window_start_dt=datetime(2025, 3, 11),
        output_window_end_dt=datetime(2025, 3, 11),
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)
    print_jointype_legend()

    expected = spark.createDataFrame([], schema=actual.schema)
    actual = actual.select(expected.columns).orderBy("TrxId")
    assert_sparkframes_equal(actual, expected, sort_keys="TrxId")


def test_left_anti_join_march_12(spark: SparkSession):
    df_a, df_b = create_example_data(spark)

    settings = IncJoinSettings(
        inc_col_name="RecDate",
        include_waiting=False,
        enforce_sliding_join_window=True,
    )
    actual = inc_join(
        df_a,
        df_b,
        how="left_anti",
        join_cols="TrxId",
        look_back_time=3,
        max_waiting_time=5,
        other_settings=settings,
        output_window_start_dt=datetime(2025, 3, 12),
        output_window_end_dt=datetime(2025, 3, 12),
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)
    print_jointype_legend()

    expected = spark.createDataFrame(
        [
            Row(
                TrxId=5,
                RecDate=date(2025, 3, 12),
                TrxId_A=5,
                RecDate_A=date(2025, 3, 7),
                TrxId_B=5,
                RecDate_B=date(2025, 3, 12),
                DiffArrivalTime=5,
                WaitingTime=None,
                JoinType="b_late",
            ),
            Row(
                TrxId=6,
                RecDate=date(2025, 3, 12),
                TrxId_A=6,
                RecDate_A=date(2025, 3, 7),
                TrxId_B=None,
                RecDate_B=None,
                DiffArrivalTime=None,
                WaitingTime=5,
                JoinType="a_timed_out",
            ),
        ],
        schema=actual.schema,
    )
    actual = actual.select(expected.columns).orderBy("TrxId")
    assert_sparkframes_equal(actual, expected, sort_keys="TrxId")


def test_left_anti_join_march_13(spark: SparkSession):
    df_a, df_b = create_example_data(spark)

    settings = IncJoinSettings(
        inc_col_name="RecDate",
        include_waiting=False,
        enforce_sliding_join_window=True,
    )
    actual = inc_join(
        df_a,
        df_b,
        how="left_anti",
        join_cols="TrxId",
        look_back_time=3,
        max_waiting_time=5,
        other_settings=settings,
        output_window_start_dt=datetime(2025, 3, 13),
        output_window_end_dt=datetime(2025, 3, 13),
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)
    print_jointype_legend()

    expected = spark.createDataFrame([], schema=actual.schema)
    actual = actual.select(expected.columns).orderBy("TrxId")
    assert_sparkframes_equal(actual, expected, sort_keys="TrxId")


def test_left_anti_join_include_waiting_records(spark: SparkSession):
    """Test that waiting records (not timed out) are included when include_waiting=True"""
    df_a, df_b = create_example_data(spark)

    settings = IncJoinSettings(
        inc_col_name="RecDate",
        include_waiting=True,
        enforce_sliding_join_window=True,
    )
    actual = inc_join(
        df_a,
        df_b,
        how="left_anti",
        join_cols="TrxId",
        look_back_time=3,
        max_waiting_time=5,
        other_settings=settings,
        output_window_start_dt=datetime(2025, 3, 1),
        output_window_end_dt=datetime(
            2025, 3, 9
        ),  # Small window so TrxId 5 and 6 are waiting but not timed out
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)
    print_jointype_legend()

    expected = spark.createDataFrame(
        [
            Row(TrxId=1, RecDate=date(2025, 3, 6), RecDate_A=date(2025, 3, 6), RecDate_B=date(2025, 3, 5), DiffArrivalTime=-1, WaitingTime=None, JoinType="a_late"),
            Row(TrxId=2, RecDate=date(2025, 3, 6), RecDate_A=date(2025, 3, 6), RecDate_B=date(2025, 3, 4), DiffArrivalTime=-2, WaitingTime=None, JoinType="a_late"),
            Row(TrxId=3, RecDate=date(2025, 3, 6), RecDate_A=date(2025, 3, 6), RecDate_B=date(2025, 3, 6), DiffArrivalTime=0, WaitingTime=None, JoinType="same_time"),
            Row(TrxId=4, RecDate=date(2025, 3, 7), RecDate_A=date(2025, 3, 7), RecDate_B=date(2025, 3, 7), DiffArrivalTime=0, WaitingTime=None, JoinType="same_time"),
            Row(TrxId=5, RecDate=date(2025, 3, 9), RecDate_A=date(2025, 3, 7), RecDate_B=None, DiffArrivalTime=None, WaitingTime=2, JoinType="a_waiting"),
            Row(TrxId=6, RecDate=date(2025, 3, 9), RecDate_A=date(2025, 3, 7), RecDate_B=None, DiffArrivalTime=None, WaitingTime=2, JoinType="a_waiting"),
            Row(TrxId=7, RecDate=date(2025, 3, 8), RecDate_A=date(2025, 3, 8), RecDate_B=date(2025, 3, 6), DiffArrivalTime=-2, WaitingTime=None, JoinType="a_late"),
        ],
        schema=JOIN_RESULT_SCHEMA_WITH_WAITING,
    )
    actual = actual.select(expected.columns).orderBy("TrxId")
    assert_sparkframes_equal(actual, expected, sort_keys="TrxId")


def test_left_anti_join_waiting_vs_timed_out_records(spark: SparkSession):
    """Test distinction between waiting records and timed out records when include_waiting=True"""
    df_a, df_b = create_example_data(spark)

    settings = IncJoinSettings(
        inc_col_name="RecDate",
        include_waiting=True,
        enforce_sliding_join_window=True,
    )
    actual = inc_join(
        df_a,
        df_b,
        how="left_anti",
        join_cols="TrxId",
        look_back_time=3,
        max_waiting_time=5,
        other_settings=settings,
        output_window_start_dt=datetime(2025, 3, 1),
        output_window_end_dt=datetime(
            2025, 3, 20
        ),  # Larger window, TrxId 6 should time out
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)
    print_jointype_legend()

    expected = spark.createDataFrame(
        [
            Row(TrxId=1, RecDate=date(2025, 3, 6), RecDate_A=date(2025, 3, 6), RecDate_B=date(2025, 3, 5), DiffArrivalTime=-1, WaitingTime=None, JoinType="a_late"),
            Row(TrxId=2, RecDate=date(2025, 3, 6), RecDate_A=date(2025, 3, 6), RecDate_B=date(2025, 3, 4), DiffArrivalTime=-2, WaitingTime=None, JoinType="a_late"),
            Row(TrxId=3, RecDate=date(2025, 3, 6), RecDate_A=date(2025, 3, 6), RecDate_B=date(2025, 3, 6), DiffArrivalTime=0, WaitingTime=None, JoinType="same_time"),
            Row(TrxId=4, RecDate=date(2025, 3, 7), RecDate_A=date(2025, 3, 7), RecDate_B=date(2025, 3, 7), DiffArrivalTime=0, WaitingTime=None, JoinType="same_time"),
            Row(TrxId=5, RecDate=date(2025, 3, 12), RecDate_A=date(2025, 3, 7), RecDate_B=date(2025, 3, 12), DiffArrivalTime=5, WaitingTime=None, JoinType="b_late"),
            Row(TrxId=6, RecDate=date(2025, 3, 12), RecDate_A=date(2025, 3, 7), RecDate_B=None, DiffArrivalTime=None, WaitingTime=5, JoinType="a_timed_out"),
            Row(TrxId=7, RecDate=date(2025, 3, 8), RecDate_A=date(2025, 3, 8), RecDate_B=date(2025, 3, 6), DiffArrivalTime=-2, WaitingTime=None, JoinType="a_late"),
        ],
        schema=JOIN_RESULT_SCHEMA_WITH_WAITING,
    )
    actual = actual.select(expected.columns).orderBy("TrxId")
    assert_sparkframes_equal(actual, expected, sort_keys="TrxId")
