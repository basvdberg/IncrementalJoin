from dataclasses import dataclass
import datetime
from typing import Optional, Union
from pyspark.sql import DataFrame, Column, functions as F
from pyspark.sql.types import TimestampType
from functools import reduce
from operator import and_

@dataclass
class IncJoinSettings:
    """
    You can use this class to specify non default settings for the inc_join function below. 

    Attributes:
        alias_a (str): Alias for the first dataset (df_a) to disambiguate column names if conflicts occur. Defaults to 'A'.
        alias_b (str): Alias for the second dataset (df_b) to disambiguate column names if conflicts occur. Defaults to 'B'.
        include_waiting (bool): If True, include unmatched rows from df_a in the output(waiting records). Defaults to False.
        inc_col_name (str): Incremental column name. Column that identifies each increment. Should be sequential 
                             and of date or datetime type. (Numbers are not supported at this time). 
                             Defaults to 'RecordDT'. Should exist in both datasets.
        time_uom (str): Time unit of measure for the incremental column. Currently only 'day' is supported.
        enforce_sliding_join_window (bool): Default is True. If True, the sliding join window will be enforced. This means that  
        the output will be the same regardless of the size of the output window. So even when joining e.g. a year, the records in A will 
        only be matched with B, when B is part of the sliding join window of A.  
        Advantage of setting this to True is that the output will always stay the same when for example reloading an interval 
        using a large output window (e.g. a year), that was previously loaded using a small output window (e.g. a day).
        Advantage of setting this to False is that using a larger output window might produce more matches in B. 
        However, this also means that the data can change. 
    """
    alias_a: str = 'A'
    alias_b: str = 'B'
    include_waiting: bool = False
    inc_col_name: str = 'RecordDT'
    time_uom: str = 'day'
    enforce_sliding_join_window: bool = True

    def __post_init__(self) -> None:
        if self.time_uom not in ['day']:
            raise ValueError("Invalid time unit of measure. Currently only 'day' uom is supported.")

def inc_join(
    df_a: DataFrame,
    df_b: DataFrame,
    how: str = 'inner',
    join_cols: Union[str, list] = None,
    join_cond: Optional[Union[str, Column]] = None,

    # define the sliding join window:
    look_back_time: int = 0, # default is 0 days
    waiting_time: int = 0, # default is 0 days

    # define the output window:
    output_window_start_dt: datetime.datetime = None,
    output_window_end_dt: datetime.datetime = None,
    
    # to reduce the amount of parameters, other settings are passed as an IncJoinSettings object
    other_settings: Optional[IncJoinSettings] = None,
) -> DataFrame:
    """
    Perform an incremental join between two PySpark DataFrames.
    
    This function joins two incrementally refreshed tables, taking into account the refresh 
    timestamp of each table and the fact that data might arrive late. The join uses a sliding 
    join window to handle late arrivals and an output window to control which records are 
    included in the result.
    
    The sliding join window defines how we filter df_b when joining with df_a:
    `sliding_join_window(df_a) = df_a.[inc_col_name] - look_back_time till df_a.[inc_col_name] + waiting_time`
    
    The output window defines the interval for which we want to generate the output. Records 
    in the output will have their [inc_col_name] contained within the output window.

    Args:
        df_a (DataFrame): The first dataset (typically the primary dataset).
        df_b (DataFrame): The second dataset (typically the secondary dataset to join with).
        how (str, optional): Type of join. Must be one of: 'inner', 'cross', 'outer', 'full', 
                             'fullouter', 'full_outer', 'left', 'leftouter', 'left_outer', 
                             'right', 'rightouter', 'right_outer', 'semi', 'leftsemi', 
                             'left_semi', 'anti', 'leftanti', 'left_anti'. Defaults to 'inner'.
        join_cols (Union[str, list], optional): Column(s) to join on. Should exist in both 
                                                 datasets. Either join_cols or join_cond must be provided.
        join_cond (Optional[Union[str, Column]], optional): Extra join condition as a SQL expression string 
                                             or PySpark Column. Use aliases from IncJoinSettings (default 'A' and 'B') 
                                             to refer to df_a and df_b columns.
        look_back_time (int): Look-back interval in days to include late arrivals from df_b. 
                             This defines how far back in time to look for matches 
                             when df_b arrives before df_a. Defaults to 0.
        waiting_time (int): Waiting interval in days to wait for late arrivals from df_b. 
                           This defines how long to wait for matches when df_b 
                           arrives after df_a. Defaults to 0.
        output_window_start_dt (Optional[datetime.datetime]): Start datetime of the output window. 
                                                               Records in the output will have 
                                                               [inc_col_name] >= output_window_start_dt. 
                                                               Defaults to None (no lower bound).
        output_window_end_dt (Optional[datetime.datetime]): End datetime of the output window. 
                                                             Records in the output will have 
                                                             [inc_col_name] <= output_window_end_dt. 
                                                             Defaults to None (no upper bound).
        other_settings (Optional[IncJoinSettings]): An IncJoinSettings object containing advanced 
                                                     join options:
            - alias_a (str): Alias for df_a columns if conflicts occur. Defaults to 'A'.
            - alias_b (str): Alias for df_b columns if conflicts occur. Defaults to 'B'.
            - include_waiting (bool): If True, include unmatched rows from df_a in the output 
                                      (waiting/timed out records). Defaults to False.
            - inc_col_name (str): Incremental column name. Column that identifies each increment. 
                                   Should be sequential and of date or datetime type. 
                                   (Numbers are not supported at this time). Defaults to 'RecordDT'. 
                                   Should exist in both datasets.

    Returns:
        DataFrame: The result of the incremental join, including:
            - All columns from df_a (with alias suffix if conflicts exist).
            - All columns from df_b (with alias suffix if conflicts exist).
            - inc_col_name: Maximum of df_a and df_b incremental column values when matched, or 
              df_a.[inc_col_name] + waiting_time if unmatched (timed out). Always contained 
              within the output window.
            - DiffArrivalTimeDays: Difference in days between df_b and df_a incremental column values:
                - 0 if both arrive at the same time.
                - >0 if df_b is late (B.[inc_col_name] > A.[inc_col_name]).
                - <0 if df_b is early or df_a is late (B.[inc_col_name] < A.[inc_col_name]).
                - None if df_a is unmatched (timed out).

    Join Scenarios:
        The function handles four join scenarios:
        1. Same time: A and B arrived at the same time (delta_arrival_time == 0).
        2. A is late: A arrived later than B (delta_arrival_time < 0).
        3. B is late: B arrived later than A (delta_arrival_time > 0).
        4. A is timed out: No match found in df_b after waiting_time (B.[inc_col_name] is None).

    """
    # Validate that look_back_time and waiting_time are non-negative
    if look_back_time is None or look_back_time < 0:
        raise ValueError("look_back_time must be a non-negative integer (>= 0)")
    if waiting_time is None or waiting_time < 0:
        raise ValueError("waiting_time must be a non-negative integer (>= 0)")
    
    if other_settings is None:
        settings = IncJoinSettings()  # use defaults
    else:
        settings = other_settings

    a = df_a.alias(settings.alias_a)
    b = df_b.alias(settings.alias_b)

    # Apply output window filters on df_a 
    if output_window_start_dt is not None:
        # For "B is late" and "A is timed out" scenario, we need to extend backwards by waiting_time
        extended_start = output_window_start_dt - datetime.timedelta(days=waiting_time)
        a = a.filter(F.col(settings.inc_col_name) >= F.lit(extended_start))
    if output_window_end_dt is not None:
        a = a.filter(F.col(settings.inc_col_name) <= F.lit(output_window_end_dt))

    # Build join expressions on join_cols and join_cond
    join_exprs = []
    if join_cols:
        if isinstance(join_cols, str):
            join_cols = [join_cols]
        join_exprs.extend([a[c] == b[c] for c in join_cols])
    if join_cond is not None:
        if isinstance(join_cond, str):
            join_exprs.append(F.expr(join_cond))
        elif isinstance(join_cond, Column):
            join_exprs.append(join_cond)
        else:
            raise TypeError("join_cond must be a SQL string or a pyspark.sql.Column")
    
    # Apply sliding join window (optional):
    # If enforced, B.[inc_col_name] must be in [A.[inc_col_name] - look_back_time, A.[inc_col_name] + waiting_time]
    inc_col_a = a[settings.inc_col_name]
    inc_col_b = b[settings.inc_col_name]
    
    # Truncate datetime to date when time_uom is 'day' for proper day-based comparisons
    if settings.time_uom == 'day':
        # Check if columns are timestamp/datetime types by inspecting schema
        # F.to_date() is safe to use on both date and timestamp types
        a_col_type = df_a.schema[settings.inc_col_name].dataType
        b_col_type = df_b.schema[settings.inc_col_name].dataType
        
        if isinstance(a_col_type, TimestampType):
            inc_col_a = F.to_date(inc_col_a)
        if isinstance(b_col_type, TimestampType):
            inc_col_b = F.to_date(inc_col_b)

    if settings.enforce_sliding_join_window:
        # Lower bound: B.[inc_col_name] >= A.[inc_col_name] - look_back_time
        join_exprs.append(inc_col_b >= F.date_sub(inc_col_a, look_back_time))

        # Upper bound: B.[inc_col_name] <= A.[inc_col_name] + waiting_time
        join_exprs.append(inc_col_b <= F.date_add(inc_col_a, waiting_time))
    
    if not join_exprs:
        raise ValueError("Either join_cols or join_cond must be provided.")

    final_join_condition = reduce(and_, join_exprs)
    result = a.join(b, final_join_condition, how)

    # Automatic column conflict resolution
    a_cols = set(df_a.columns)
    b_cols = set(df_b.columns)
    common_cols = a_cols & b_cols - {settings.inc_col_name}
    for col in common_cols:
        if col in result.columns:
            if f"{col}_{settings.alias_a}" not in result.columns:
                result = result.withColumnRenamed(col, f"{col}_{settings.alias_a}")
            if col in b_cols and f"{col}_{settings.alias_b}" not in result.columns:
                result = result.withColumnRenamed(col, f"{col}_{settings.alias_b}")

    # Compute DiffArrivalTimeDays: B.[inc_col_name] - A.[inc_col_name] in days
    result = result.withColumn(
        "DiffArrivalTimeDays",
        F.when(inc_col_b.isNotNull(),
               F.datediff(inc_col_b, inc_col_a))
         .otherwise(None)
    )

    # By default, waiting records are not included in the output, 
    # because the output contains only matched records or timed out records.
    # but you can override this by behaviour via the include_waiting setting.
    if not settings.include_waiting:
        result = result.filter(F.col("DiffArrivalTimeDays").isNotNull())   

    # Compute final inc_col_name
    # When matched: max(A.[inc_col_name], B.[inc_col_name])
    # When timed out: A.[inc_col_name] + waiting_time
    result = result.withColumn(
        settings.inc_col_name,
        F.when(inc_col_b.isNotNull(),
               F.greatest(inc_col_a, inc_col_b))
         .otherwise(F.expr(f"date_add({settings.alias_a}.{settings.inc_col_name}, {waiting_time})"))
    )

    return result
