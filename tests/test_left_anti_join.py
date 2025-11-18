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
        Row(
            TrxDT=datetime(2025, 3, 10, 9, 0, 0),
            CreditDebit="Credit",
            AmountEuro=Decimal(99.99),
            AccountName="Mr. X",
            TrxId=8,
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
        output_window_start=date(2025, 3, 1),
        output_window_end=date(2025, 3, 30),
    )
    joined = joined.orderBy("TrxId")
    joined.show(truncate=True)

    expected = spark.createDataFrame(
        [
            Row(TrxId=2, RecDate=date(2025, 3, 6), RecDate_A=date(2025, 3, 6)),
            Row(TrxId=5, RecDate=date(2025, 3, 7), RecDate_A=date(2025, 3, 7)),
            Row(TrxId=6, RecDate=date(2025, 3, 7), RecDate_A=date(2025, 3, 7)),
            Row(TrxId=7, RecDate=date(2025, 3, 8), RecDate_A=date(2025, 3, 8)),
            Row(TrxId=8, RecDate=date(2025, 3, 8), RecDate_A=date(2025, 3, 8)),
        ],
        schema="TrxId INT, RecDate DATE, RecDate_A DATE",
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
        output_window_start=date(2025, 3, 1),
        output_window_end=date(2025, 3, 30),
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)

    expected = spark.createDataFrame(
        [
            Row(TrxId=8, RecDate=date(2025, 3, 13), RecDate_A=date(2025, 3, 8)),
        ],
        schema="TrxId INT, RecDate DATE, RecDate_A DATE",
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
        max_waiting_time=2,
        other_settings=settings,
        output_window_start=date(2025, 3, 1),
        output_window_end=date(2025, 3, 9),
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)
    # 5 and 6 are in the output because max waiting is not big enough
    expected = spark.createDataFrame(
        [
            Row(TrxId=5, RecDate=date(2025, 3, 9), RecDate_A=date(2025, 3, 7)),
            Row(TrxId=6, RecDate=date(2025, 3, 9), RecDate_A=date(2025, 3, 7)),
        ],
        schema="TrxId INT, RecDate DATE, RecDate_A DATE",
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
        look_back_time=1,
        max_waiting_time=0,
        other_settings=settings,
        output_window_start=date(2025, 3, 6),
        output_window_end=date(2025, 3, 6),
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)

    expected = spark.createDataFrame(
        [
            Row(TrxId=2, RecDate=date(2025, 3, 6), RecDate_A=date(2025, 3, 6)),
        ],
        schema="TrxId INT, RecDate DATE, RecDate_A DATE",
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
        look_back_time=1,
        max_waiting_time=0,
        other_settings=settings,
        output_window_start=date(2025, 3, 7),
        output_window_end=date(2025, 3, 7),
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)

    expected = spark.createDataFrame(
        [
            Row(TrxId=5, RecDate=date(2025, 3, 7), RecDate_A=date(2025, 3, 7)),
            Row(TrxId=6, RecDate=date(2025, 3, 7), RecDate_A=date(2025, 3, 7)),
        ],
        schema="TrxId INT, RecDate DATE, RecDate_A DATE",
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
        look_back_time=1,
        max_waiting_time=0,
        other_settings=settings,
        output_window_start=date(2025, 3, 8),
        output_window_end=date(2025, 3, 8),
    )
    actual = actual.orderBy("TrxId")
    actual.show(truncate=True)

    expected = spark.createDataFrame(
        [
            Row(TrxId=7, RecDate=date(2025, 3, 8), RecDate_A=date(2025, 3, 8)),
            Row(TrxId=8, RecDate=date(2025, 3, 8), RecDate_A=date(2025, 3, 8)),
        ],
        schema="TrxId INT, RecDate DATE, RecDate_A DATE",
    )
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
                TrxId=5,
                RecDate=date(2025, 3, 9),
                RecDate_A=date(2025, 3, 7),
                WaitingTime=2,
            ),
            Row(
                TrxId=6,
                RecDate=date(2025, 3, 9),
                RecDate_A=date(2025, 3, 7),
                WaitingTime=2,
            ),
            Row(
                TrxId=8,
                RecDate=date(2025, 3, 9),
                RecDate_A=date(2025, 3, 8),
                WaitingTime=1,
            ),
        ],
        schema="TrxId INT, RecDate DATE, RecDate_A DATE, WaitingTime INT",
    )
    actual = actual.select(expected.columns).orderBy("TrxId")
    assert_sparkframes_equal(actual, expected, sort_keys="TrxId")
