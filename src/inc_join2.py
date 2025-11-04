from dataclasses import dataclass
import datetime
from typing import Optional, Union
from pyspark.sql import DataFrame, Column, functions as F
from functools import reduce
from operator import and_

from pyspark.sql.types import T

@dataclass
class IncJoinSettings:
    """
    Settings for performing an incremental join between two PySpark DataFrames using the inc_join function below. 

    Attributes:
        alias_a (str): Alias for the first dataset (df_a) to disambiguate column names if conflicts occur. Defaults to 'A'.
        alias_b (str): Alias for the second dataset (df_b) to disambiguate column names if conflicts occur. Defaults to 'B'.
        include_waiting (bool): If True, include unmatched rows from df_a (waiting records). Defaults to False.
        timestamp_col_name (str): Name of the incremental timestamp column. Defaults to 'RecordDT'.
    """
    alias_a: str = 'A'
    alias_b: str = 'B'
    include_waiting: bool = False
    timestamp_col_name: str = 'RecordDT'


def inc_join(
    df_a: DataFrame,
    df_b: DataFrame,
    how: str = 'inner',
    join_cols: Union[str, list] = None,
    join_cond: Optional[str] = None,

    look_back_days: int = 0,
    waiting_days: int = 0,
    join_window_start_dt: datetime = None,
    join_window_end_dt: datetime = None,
    other_settings: Optional[IncJoinSettings] = None,
) -> DataFrame:
    """
    Perform an incremental join between two PySpark DataFrames.

    Args:
        df_a (DataFrame): The first dataset.
        df_b (DataFrame): The second dataset.
        how (str, optional): Type of join. Must be one of: 'inner', 'cross', 'outer', 'full', 'fullouter', 'full_outer',
                             'left', 'leftouter', 'left_outer', 'right', 'rightouter', 'right_outer', 'semi',
                             'leftsemi', 'left_semi', 'anti', 'leftanti', 'left_anti'. Defaults to 'inner'.
        join_cols (str or list, optional): Column(s) to join on. Should exist in both datasets.
        join_condition (str, optional): Extra join condition. Use aliases `a` and `b` to refer to `df_a` and `df_b`.
        waiting_days (int): Days to add if a row from df_a has no match in df_b.
        look_back_days (int): Look-back interval in days to include late arrivals from df_b. Defaults to 0.
        join_window_start_dt (Optional[datetime]): Start datetime string to filter df_a before joining. Defaults to None.
        join_window_end_dt (Optional[datetime]): End datetime string to filter df_a before joining. Defaults to None.
        settings (IncJoinSettings, optional): An IncJoinSettings object containing advanced join options:
            - alias_a (str): Alias for df_a columns if conflicts occur.
            - alias_b (str): Alias for df_b columns if conflicts occur.
            - include_waiting (bool): Include unmatched rows from df_a if True.
            - timestamp_col_name (str): Column name used for incremental timestamps.

    Returns:
        DataFrame: The result of the incremental join, including:
            - All columns from df_a (with alias suffix if conflicts exist).
            - All columns from df_b (with alias suffix if conflicts exist).
            - timestamp_col_name: Maximum of df_a and df_b timestamps, or df_a + waiting_interval_days if unmatched.
            - DiffArrivalTimeDays: Difference in days between df_b and df_a timestamps:
                - 0 if both arrive at the same time.
                - >0 if df_b is late.
                - <0 if df_b is early or df_a is late.
                - None if df_a is unmatched (timed out).

    Example:
        settings = IncJoinSettings(
            alias_a='A',
            alias_b='B',
            include_waiting=True,
            timestamp_col_name='RecordDT',
        )

        result = inc_join(df_a, df_b, how='left', join_cols=['id'], settings=settings)
    """
    if settings is None:
        settings = IncJoinSettings()  # use defaults

    a = df_a.alias(settings.alias_a)
    b = df_b.alias(settings.alias_b)

    # Apply join window filters on df_a
    if settings.join_window_start_dt:
        a = a.filter(F.col(settings.timestamp_col_name) >= F.lit(settings.join_window_start_dt))
    if settings.join_window_end_dt:
        a = a.filter(F.col(settings.timestamp_col_name) <= F.lit(settings.join_window_end_dt))

    # Build join expressions
    join_exprs = []
    if join_cols:
        if isinstance(join_cols, str):
            join_cols = [join_cols]
        join_exprs.extend([a[c] == b[c] for c in join_cols])
    if join_cond is not None:
        join_exprs.append(join_cond)
    if settings.look_back_interval > 0:
        join_exprs.append(b[settings.timestamp_col_name] >= F.date_sub(a[settings.timestamp_col_name], settings.look_back_interval))
    if not join_exprs:
        raise ValueError("Either join_cols or join_condition must be provided.")

    final_join_condition = reduce(and_, join_exprs)

    # Determine join type
    join_type = 'left' if settings.include_waiting else how
    result = a.join(b, final_join_condition, join_type)

    # Automatic column conflict resolution
    a_cols = set(df_a.columns)
    b_cols = set(df_b.columns)
    common_cols = a_cols & b_cols - {settings.timestamp_col_name}
    for col in common_cols:
        if col in result.columns:
            if f"{col}_{settings.alias_a}" not in result.columns:
                result = result.withColumnRenamed(col, f"{col}_{settings.alias_a}")
            if col in b.columns and f"{col}_{settings.alias_b}" not in result.columns:
                result = result.withColumnRenamed(col, f"{col}_{settings.alias_b}")

    # Compute DiffArrivalTimeDays
    result = result.withColumn(
        "DiffArrivalTimeDays",
        F.when(b[settings.timestamp_col_name].isNotNull(),
               F.datediff(b[settings.timestamp_col_name], a[settings.timestamp_col_name]))
         .otherwise(None)
    )

    # Compute final timestamp_col_name
    result = result.withColumn(
        settings.timestamp_col_name,
        F.when(b[settings.timestamp_col_name].isNotNull(),
               F.greatest(a[settings.timestamp_col_name], b[settings.timestamp_col_name]))
         .otherwise(F.expr(f"date_add({settings.alias_a}.{settings.timestamp_col_name}, {settings.waiting_interval_days})"))
    )

    return result
