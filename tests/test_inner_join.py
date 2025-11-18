from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import Row, SparkSession

from inc_join import IncJoinSettings, inc_join
from tests.test_utils import assert_sparkframes_equal


def create_example_data(spark: SparkSession):
    schema = "TrxDT Timestamp, CreditDebit String, AmountEuro Decimal(12,2), AccountName String, TrxId Integer, RecDate Date"
    fin_data = [
        Row(
            TrxDT=datetime(2025, 3, 6, 20, 45, 19),
            CreditDebit="Credit",
            AmountEuro=Decimal(700.30),
            AccountName="Mrs. Zsa Zsa",
            TrxId=1,
            RecDate=date(2025, 3, 6),
        ),
        Row(
            TrxDT=datetime(2025, 3, 6, 12, 22, 1),
            CreditDebit="Debit",
            AmountEuro=Decimal(200.00),
            AccountName="Mrs. Zsa Zsa",
            TrxId=2,
            RecDate=date(2025, 3, 6),
        ),
        Row(
            TrxDT=datetime(2025, 3, 6, 20, 59, 0),
            CreditDebit="Debit",
            AmountEuro=Decimal(1110.20),
            AccountName="Mrs. Zsa Zsa",
            TrxId=3,
            RecDate=date(2025, 3, 6),
        ),
        Row(
            TrxDT=datetime(2025, 3, 6, 23, 50, 0),
            CreditDebit="Credit",
            AmountEuro=Decimal(50.00),
            AccountName="Mrs. Zsa Zsa",
            TrxId=4,
            RecDate=date(2025, 3, 7),
        ),
        Row(
            TrxDT=datetime(2025, 3, 6, 8, 0, 0),
            CreditDebit="Credit",
            AmountEuro=Decimal(1500.00),
            AccountName="Mr. X",
            TrxId=5,
            RecDate=date(2025, 3, 7),
        ),
        Row(
            TrxDT=datetime(2025, 3, 7, 14, 45, 0),
            CreditDebit="Debit",
            AmountEuro=Decimal(300.25),
            AccountName="Mr. X",
            TrxId=6,
            RecDate=date(2025, 3, 7),
        ),
        Row(
            TrxDT=datetime(2025, 3, 10, 9, 0, 0),
            CreditDebit="Credit",
            AmountEuro=Decimal(99.99),
            AccountName="Mr. X",
            TrxId=7,
            RecDate=date(2025, 3, 8),
        ),
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


# Test look back
# When look back is 1 Trx 1 should be looked back, but 2 and 7 should not.
# Waiting time is 0, so 5 and 6 should not be in the output.
# Of course 3 and 4 should be in the output, because they are on time.
def test_inner_join_look_back_eq_1(spark: SparkSession):
    df_a, df_b = create_example_data(spark)
    settings = IncJoinSettings(
        inc_col_name="RecDate",
        include_waiting=False,
        enforce_sliding_join_window=True,
    )
    joined = inc_join(
        df_a,
        df_b,
        how="inner",
        join_cols="TrxId",
        look_back_time=1,
        max_waiting_time=0,
        other_settings=settings,
        output_window_start=date(2025, 3, 1),
        output_window_end=date(2025, 3, 30),
    )
    joined = joined.orderBy("TrxId")
    joined.show(truncate=True)

    expected = spark.createDataFrame(
        [
            Row(
                TrxId=1,
                RecDate=date(2025, 3, 6),
                RecDate_A=date(2025, 3, 6),
                RecDate_B=date(2025, 3, 5),
                DiffArrivalTime=-1,
                JoinType="a_late",
            ),
            Row(
                TrxId=3,
                RecDate=date(2025, 3, 6),
                RecDate_A=date(2025, 3, 6),
                RecDate_B=date(2025, 3, 6),
                DiffArrivalTime=0,
                JoinType="same_time",
            ),
            Row(
                TrxId=4,
                RecDate=date(2025, 3, 7),
                RecDate_A=date(2025, 3, 7),
                RecDate_B=date(2025, 3, 7),
                DiffArrivalTime=0,
                JoinType="same_time",
            ),
        ],
        schema="TrxId INT, RecDate DATE, RecDate_A DATE, RecDate_B DATE, DiffArrivalTime INT, JoinType STRING",
    )
    actual = joined.select(expected.columns).orderBy("TrxId")
    assert_sparkframes_equal(actual, expected, sort_keys="TrxId")


def test_inner_join_no_enforce_sliding_window(spark: SparkSession):
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
        how="inner",
        join_cols="TrxId",
        look_back_time=3,
        max_waiting_time=5,
        other_settings=settings,
        output_window_start=date(2025, 3, 1),
        output_window_end=date(2025, 3, 30),
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)

    expected = spark.createDataFrame(
        [
            Row(
                TrxId=1,
                RecDate=date(2025, 3, 6),
                RecDate_A=date(2025, 3, 6),
                RecDate_B=date(2025, 3, 5),
                DiffArrivalTime=-1,
                JoinType="a_late",
            ),
            Row(
                TrxId=2,
                RecDate=date(2025, 3, 6),
                RecDate_A=date(2025, 3, 6),
                RecDate_B=date(2025, 3, 4),
                DiffArrivalTime=-2,
                JoinType="a_late",
            ),
            Row(
                TrxId=3,
                RecDate=date(2025, 3, 6),
                RecDate_A=date(2025, 3, 6),
                RecDate_B=date(2025, 3, 6),
                DiffArrivalTime=0,
                JoinType="same_time",
            ),
            Row(
                TrxId=4,
                RecDate=date(2025, 3, 7),
                RecDate_A=date(2025, 3, 7),
                RecDate_B=date(2025, 3, 7),
                DiffArrivalTime=0,
                JoinType="same_time",
            ),
            Row(
                TrxId=5,
                RecDate=date(2025, 3, 12),
                RecDate_A=date(2025, 3, 7),
                RecDate_B=date(2025, 3, 12),
                DiffArrivalTime=5,
                JoinType="b_late",
            ),
            Row(
                TrxId=6,
                RecDate=date(2025, 3, 18),
                RecDate_A=date(2025, 3, 7),
                RecDate_B=date(2025, 3, 18),
                DiffArrivalTime=11,
                JoinType="b_late",
            ),
            Row(
                TrxId=7,
                RecDate=date(2025, 3, 8),
                RecDate_A=date(2025, 3, 8),
                RecDate_B=date(2025, 3, 6),
                DiffArrivalTime=-2,
                JoinType="a_late",
            ),
        ],
        schema="TrxId INT, RecDate DATE, RecDate_A DATE, RecDate_B DATE, DiffArrivalTime INT, JoinType STRING",
    )
    actual = actual.select(expected.columns).orderBy("TrxId")
    assert_sparkframes_equal(actual, expected, sort_keys="TrxId")


#  because we inner join, the timed out rows are not included.
def test_inner_join_timed_out_rows(spark: SparkSession):
    df_a, df_b = create_example_data(spark)

    settings = IncJoinSettings(
        inc_col_name="RecDate",
        include_waiting=False,
        enforce_sliding_join_window=True,
    )
    actual = inc_join(
        df_a,
        df_b,
        how="inner",
        join_cols="TrxId",
        look_back_time=3,
        max_waiting_time=5,
        other_settings=settings,
        output_window_start=date(2025, 3, 1),
        output_window_end=date(2025, 3, 20),
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)

    expected = spark.createDataFrame(
        [
            Row(
                TrxId=1,
                RecDate=date(2025, 3, 6),
                RecDate_A=date(2025, 3, 6),
                RecDate_B=date(2025, 3, 5),
                DiffArrivalTime=-1,
                JoinType="a_late",
            ),
            Row(
                TrxId=2,
                RecDate=date(2025, 3, 6),
                RecDate_A=date(2025, 3, 6),
                RecDate_B=date(2025, 3, 4),
                DiffArrivalTime=-2,
                JoinType="a_late",
            ),
            Row(
                TrxId=3,
                RecDate=date(2025, 3, 6),
                RecDate_A=date(2025, 3, 6),
                RecDate_B=date(2025, 3, 6),
                DiffArrivalTime=0,
                JoinType="same_time",
            ),
            Row(
                TrxId=4,
                RecDate=date(2025, 3, 7),
                RecDate_A=date(2025, 3, 7),
                RecDate_B=date(2025, 3, 7),
                DiffArrivalTime=0,
                JoinType="same_time",
            ),
            Row(
                TrxId=5,
                RecDate=date(2025, 3, 12),
                RecDate_A=date(2025, 3, 7),
                RecDate_B=date(2025, 3, 12),
                DiffArrivalTime=5,
                JoinType="b_late",
            ),
            Row(
                TrxId=7,
                RecDate=date(2025, 3, 8),
                RecDate_A=date(2025, 3, 8),
                RecDate_B=date(2025, 3, 6),
                DiffArrivalTime=-2,
                JoinType="a_late",
            ),
        ],
        schema="TrxId INT, RecDate DATE, RecDate_A DATE, RecDate_B DATE, DiffArrivalTime INT, JoinType STRING",
    )
    actual = actual.select(expected.columns).orderBy("TrxId")
    assert_sparkframes_equal(actual, expected, sort_keys="TrxId")


def test_inner_join_small_output_window(spark: SparkSession):
    df_a, df_b = create_example_data(spark)

    settings = IncJoinSettings(
        inc_col_name="RecDate",
        include_waiting=False,
        enforce_sliding_join_window=True,
    )
    actual = inc_join(
        df_a,
        df_b,
        how="inner",
        join_cols="TrxId",
        look_back_time=3,
        max_waiting_time=5,
        other_settings=settings,
        output_window_start=date(2025, 3, 1),
        output_window_end=date(2025, 3, 9),
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)

    expected = spark.createDataFrame(
        [
            Row(
                TrxId=1,
                RecDate=date(2025, 3, 6),
                RecDate_A=date(2025, 3, 6),
                RecDate_B=date(2025, 3, 5),
                DiffArrivalTime=-1,
                JoinType="a_late",
            ),
            Row(
                TrxId=2,
                RecDate=date(2025, 3, 6),
                RecDate_A=date(2025, 3, 6),
                RecDate_B=date(2025, 3, 4),
                DiffArrivalTime=-2,
                JoinType="a_late",
            ),
            Row(
                TrxId=3,
                RecDate=date(2025, 3, 6),
                RecDate_A=date(2025, 3, 6),
                RecDate_B=date(2025, 3, 6),
                DiffArrivalTime=0,
                JoinType="same_time",
            ),
            Row(
                TrxId=4,
                RecDate=date(2025, 3, 7),
                RecDate_A=date(2025, 3, 7),
                RecDate_B=date(2025, 3, 7),
                DiffArrivalTime=0,
                JoinType="same_time",
            ),
            Row(
                TrxId=7,
                RecDate=date(2025, 3, 8),
                RecDate_A=date(2025, 3, 8),
                RecDate_B=date(2025, 3, 6),
                DiffArrivalTime=-2,
                JoinType="a_late",
            ),
        ],
        schema="TrxId INT, RecDate DATE, RecDate_A DATE, RecDate_B DATE, DiffArrivalTime INT, JoinType STRING",
    )
    actual = actual.select(expected.columns).orderBy("TrxId")
    assert_sparkframes_equal(actual, expected, sort_keys="TrxId")


def test_inner_join_march_6(spark: SparkSession):
    df_a, df_b = create_example_data(spark)

    settings = IncJoinSettings(
        inc_col_name="RecDate",
        include_waiting=False,
        enforce_sliding_join_window=True,
    )
    actual = inc_join(
        df_a,
        df_b,
        how="inner",
        join_cols="TrxId",
        look_back_time=3,
        max_waiting_time=5,
        other_settings=settings,
        output_window_start=date(2025, 3, 6),
        output_window_end=date(2025, 3, 6),
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)

    expected = spark.createDataFrame(
        [
            Row(
                TrxId=1,
                RecDate=date(2025, 3, 6),
                RecDate_A=date(2025, 3, 6),
                RecDate_B=date(2025, 3, 5),
                DiffArrivalTime=-1,
                JoinType="a_late",
            ),
            Row(
                TrxId=2,
                RecDate=date(2025, 3, 6),
                RecDate_A=date(2025, 3, 6),
                RecDate_B=date(2025, 3, 4),
                DiffArrivalTime=-2,
                JoinType="a_late",
            ),
            Row(
                TrxId=3,
                RecDate=date(2025, 3, 6),
                RecDate_A=date(2025, 3, 6),
                RecDate_B=date(2025, 3, 6),
                DiffArrivalTime=0,
                JoinType="same_time",
            ),
        ],
        schema="TrxId INT, RecDate DATE, RecDate_A DATE, RecDate_B DATE, DiffArrivalTime INT, JoinType STRING",
    )
    actual = actual.select(expected.columns).orderBy("TrxId")
    assert_sparkframes_equal(actual, expected, sort_keys="TrxId")


def test_inner_join_march_7(spark: SparkSession):
    df_a, df_b = create_example_data(spark)

    settings = IncJoinSettings(
        inc_col_name="RecDate",
        include_waiting=False,
        enforce_sliding_join_window=True,
    )
    actual = inc_join(
        df_a,
        df_b,
        how="inner",
        join_cols="TrxId",
        look_back_time=3,
        max_waiting_time=5,
        other_settings=settings,
        output_window_start=date(2025, 3, 7),
        output_window_end=date(2025, 3, 7),
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)

    expected = spark.createDataFrame(
        [
            Row(
                TrxId=4,
                RecDate=date(2025, 3, 7),
                RecDate_A=date(2025, 3, 7),
                RecDate_B=date(2025, 3, 7),
                DiffArrivalTime=0,
                JoinType="same_time",
            ),
        ],
        schema="TrxId INT, RecDate DATE, RecDate_A DATE, RecDate_B DATE, DiffArrivalTime INT, JoinType STRING",
    )
    actual = actual.select(expected.columns).orderBy("TrxId")
    assert_sparkframes_equal(actual, expected, sort_keys="TrxId")


def test_inner_join_march_8(spark: SparkSession):
    df_a, df_b = create_example_data(spark)

    settings = IncJoinSettings(
        inc_col_name="RecDate",
        include_waiting=False,
        enforce_sliding_join_window=True,
    )
    actual = inc_join(
        df_a,
        df_b,
        how="inner",
        join_cols="TrxId",
        look_back_time=3,
        max_waiting_time=5,
        other_settings=settings,
        output_window_start=date(2025, 3, 8),
        output_window_end=date(2025, 3, 8),
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)

    expected = spark.createDataFrame(
        [
            Row(
                TrxId=7,
                RecDate=date(2025, 3, 8),
                RecDate_A=date(2025, 3, 8),
                RecDate_B=date(2025, 3, 6),
                DiffArrivalTime=-2,
                JoinType="a_late",
            ),
        ],
        schema="TrxId INT, RecDate DATE, RecDate_A DATE, RecDate_B DATE, DiffArrivalTime INT, JoinType STRING",
    )
    actual = actual.select(expected.columns).orderBy("TrxId")
    assert_sparkframes_equal(actual, expected, sort_keys="TrxId")


def test_inner_join_march_9(spark: SparkSession):
    df_a, df_b = create_example_data(spark)

    settings = IncJoinSettings(
        inc_col_name="RecDate",
        include_waiting=False,
        enforce_sliding_join_window=True,
    )
    actual = inc_join(
        df_a,
        df_b,
        how="inner",
        join_cols="TrxId",
        look_back_time=3,
        max_waiting_time=5,
        other_settings=settings,
        output_window_start=date(2025, 3, 9),
        output_window_end=date(2025, 3, 9),
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)

    expected = spark.createDataFrame([], schema=actual.schema)
    actual = actual.select(expected.columns).orderBy("TrxId")
    assert_sparkframes_equal(actual, expected, sort_keys="TrxId")


def test_inner_join_march_10(spark: SparkSession):
    df_a, df_b = create_example_data(spark)

    settings = IncJoinSettings(
        inc_col_name="RecDate",
        include_waiting=False,
        enforce_sliding_join_window=True,
    )
    actual = inc_join(
        df_a,
        df_b,
        how="inner",
        join_cols="TrxId",
        look_back_time=3,
        max_waiting_time=5,
        other_settings=settings,
        output_window_start=date(2025, 3, 10),
        output_window_end=date(2025, 3, 10),
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)

    expected = spark.createDataFrame([], schema=actual.schema)
    actual = actual.select(expected.columns).orderBy("TrxId")
    assert_sparkframes_equal(actual, expected, sort_keys="TrxId")


def test_inner_join_march_11(spark: SparkSession):
    df_a, df_b = create_example_data(spark)

    settings = IncJoinSettings(
        inc_col_name="RecDate",
        include_waiting=False,
        enforce_sliding_join_window=True,
    )
    actual = inc_join(
        df_a,
        df_b,
        how="inner",
        join_cols="TrxId",
        look_back_time=3,
        max_waiting_time=5,
        other_settings=settings,
        output_window_start=date(2025, 3, 11),
        output_window_end=date(2025, 3, 11),
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)

    expected = spark.createDataFrame([], schema=actual.schema)
    actual = actual.select(expected.columns).orderBy("TrxId")
    assert_sparkframes_equal(actual, expected, sort_keys="TrxId")


# no timed out records because we inner join.
def test_inner_join_march_12(spark: SparkSession):
    df_a, df_b = create_example_data(spark)

    settings = IncJoinSettings(
        inc_col_name="RecDate",
        include_waiting=False,
        enforce_sliding_join_window=True,
    )
    actual = inc_join(
        df_a,
        df_b,
        how="inner",
        join_cols="TrxId",
        look_back_time=3,
        max_waiting_time=5,
        other_settings=settings,
        output_window_start=date(2025, 3, 12),
        output_window_end=date(2025, 3, 12),
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)

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
            )
        ],
        schema=actual.schema,
    )
    actual = actual.select(expected.columns).orderBy("TrxId")
    assert_sparkframes_equal(actual, expected, sort_keys="TrxId")


def test_inner_join_march_13(spark: SparkSession):
    df_a, df_b = create_example_data(spark)

    settings = IncJoinSettings(
        inc_col_name="RecDate",
        include_waiting=False,
        enforce_sliding_join_window=True,
    )
    actual = inc_join(
        df_a,
        df_b,
        how="inner",
        join_cols="TrxId",
        look_back_time=3,
        max_waiting_time=5,
        other_settings=settings,
        output_window_start=date(2025, 3, 13),
        output_window_end=date(2025, 3, 13),
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)

    expected = spark.createDataFrame([], schema=actual.schema)
    actual = actual.select(expected.columns).orderBy("TrxId")
    assert_sparkframes_equal(actual, expected, sort_keys="TrxId")


# Because we inner join, there are no waiting records.
def test_inner_join_include_waiting_records(spark: SparkSession):
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
        how="inner",
        join_cols="TrxId",
        look_back_time=3,
        max_waiting_time=5,
        other_settings=settings,
        output_window_start=date(2025, 3, 1),
        output_window_end=date(
            2025, 3, 9
        ),  # Small window so TrxId 5 and 6 are waiting but not timed out
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)

    expected = spark.createDataFrame(
        [
            Row(
                TrxId=1,
                RecDate=date(2025, 3, 6),
                RecDate_A=date(2025, 3, 6),
                RecDate_B=date(2025, 3, 5),
                DiffArrivalTime=-1,
                WaitingTime=None,
                JoinType="a_late",
            ),
            Row(
                TrxId=2,
                RecDate=date(2025, 3, 6),
                RecDate_A=date(2025, 3, 6),
                RecDate_B=date(2025, 3, 4),
                DiffArrivalTime=-2,
                WaitingTime=None,
                JoinType="a_late",
            ),
            Row(
                TrxId=3,
                RecDate=date(2025, 3, 6),
                RecDate_A=date(2025, 3, 6),
                RecDate_B=date(2025, 3, 6),
                DiffArrivalTime=0,
                WaitingTime=None,
                JoinType="same_time",
            ),
            Row(
                TrxId=4,
                RecDate=date(2025, 3, 7),
                RecDate_A=date(2025, 3, 7),
                RecDate_B=date(2025, 3, 7),
                DiffArrivalTime=0,
                WaitingTime=None,
                JoinType="same_time",
            ),
            Row(
                TrxId=7,
                RecDate=date(2025, 3, 8),
                RecDate_A=date(2025, 3, 8),
                RecDate_B=date(2025, 3, 6),
                DiffArrivalTime=-2,
                WaitingTime=None,
                JoinType="a_late",
            ),
        ],
        schema="TrxId INT, RecDate DATE, RecDate_A DATE, RecDate_B DATE, DiffArrivalTime INT, WaitingTime INT, JoinType STRING",
    )
    actual = actual.select(expected.columns).orderBy("TrxId")
    assert_sparkframes_equal(actual, expected, sort_keys="TrxId")


# because we inner join, there are no timed out records.
def test_inner_join_waiting_vs_timed_out_records(spark: SparkSession):
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
        how="inner",
        join_cols="TrxId",
        look_back_time=3,
        max_waiting_time=5,
        other_settings=settings,
        output_window_start=date(2025, 3, 1),
        output_window_end=date(2025, 3, 20),  # Larger window, TrxId 6 should time out
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)

    expected = spark.createDataFrame(
        [
            Row(
                TrxId=1,
                RecDate=date(2025, 3, 6),
                RecDate_A=date(2025, 3, 6),
                RecDate_B=date(2025, 3, 5),
                DiffArrivalTime=-1,
                WaitingTime=None,
                JoinType="a_late",
            ),
            Row(
                TrxId=2,
                RecDate=date(2025, 3, 6),
                RecDate_A=date(2025, 3, 6),
                RecDate_B=date(2025, 3, 4),
                DiffArrivalTime=-2,
                WaitingTime=None,
                JoinType="a_late",
            ),
            Row(
                TrxId=3,
                RecDate=date(2025, 3, 6),
                RecDate_A=date(2025, 3, 6),
                RecDate_B=date(2025, 3, 6),
                DiffArrivalTime=0,
                WaitingTime=None,
                JoinType="same_time",
            ),
            Row(
                TrxId=4,
                RecDate=date(2025, 3, 7),
                RecDate_A=date(2025, 3, 7),
                RecDate_B=date(2025, 3, 7),
                DiffArrivalTime=0,
                WaitingTime=None,
                JoinType="same_time",
            ),
            Row(
                TrxId=5,
                RecDate=date(2025, 3, 12),
                RecDate_A=date(2025, 3, 7),
                RecDate_B=date(2025, 3, 12),
                DiffArrivalTime=5,
                WaitingTime=None,
                JoinType="b_late",
            ),
            Row(
                TrxId=7,
                RecDate=date(2025, 3, 8),
                RecDate_A=date(2025, 3, 8),
                RecDate_B=date(2025, 3, 6),
                DiffArrivalTime=-2,
                WaitingTime=None,
                JoinType="a_late",
            ),
        ],
        schema="TrxId INT, RecDate DATE, RecDate_A DATE, RecDate_B DATE, DiffArrivalTime INT, WaitingTime INT, JoinType STRING",
    )
    actual = actual.select(expected.columns).orderBy("TrxId")
    assert_sparkframes_equal(actual, expected, sort_keys="TrxId")
