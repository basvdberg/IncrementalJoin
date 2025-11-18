import datetime
import logging
from dataclasses import dataclass
from datetime import date as Date
from datetime import datetime as DateTime
from functools import reduce
from operator import and_
from typing import Dict, List, Optional, Tuple, Union

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

# Set up log for this module
log = logging.getLogger(__name__)


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
                             Defaults to 'RecDate'. Should exist in both datasets.
        time_uom (str): Time unit of measure for the incremental column. Currently only 'day' is supported.
        enforce_sliding_join_window (bool): Default is True. If True, the sliding join window will be enforced. This means that
        the output will be the same regardless of the size of the output window. So even when joining e.g. a year, the records in A will
        only be matched with B, when B is part of the sliding join window of A.
        Advantage of setting this to True is that the output will always stay the same when for example reloading an interval
        using a large output window (e.g. a year), that was previously loaded using a small output window (e.g. a day).
        Advantage of setting this to False is that using a larger output window might produce more matches in B.
        However, this also means that the data can change.
        output_select (str): Comma separated tokens that determine which columns the final output should contain.
            Tokens can include:
            - join_cols: the join columns restored to their original names.
            - inc_col: the final incremental column (settings.inc_col_name).
            - df_a_cols / df_b_cols: all columns originating from df_a / df_b (after renaming to avoid collisions).
            - DiffArrivalTime / WaitingTime / JoinType: the derived columns produced by inc_join.
            You can also provide explicit column names. Defaults to
            "join_cols, inc_col, df_a_cols, df_b_cols, DiffArrivalTime, WaitingTime, JoinType".
    """

    alias_a: str = "A"
    alias_b: str = "B"
    include_waiting: bool = False
    inc_col_name: str = "RecDate"
    time_uom: str = "day"
    enforce_sliding_join_window: bool = True
    output_select: str = "join_cols, inc_col, df_a_cols, df_b_cols, DiffArrivalTime, WaitingTime, JoinType"

    def __post_init__(self) -> None:
        if self.time_uom not in ["day"]:
            raise ValueError(
                "Invalid time unit of measure. Currently only 'day' uom is supported."
            )


def check_inc_join_params(
    join_cols: Union[str, list, None],
    join_cond: Optional[Union[str, Column]],
    look_back_time: Optional[int],
    max_waiting_time: Optional[int],
    output_window_start: Optional[Union[DateTime, Date]],
    output_window_end: Optional[Union[DateTime, Date]],
    other_settings: Optional[IncJoinSettings],
    how: str,
) -> Tuple[
    IncJoinSettings, List[str], Union[DateTime, Date], Optional[Union[str, Column]], str
]:
    """Validate and normalize the high-level configuration arguments passed to inc_join."""
    # Normalize and validate how parameter
    # Replace underscores and remove "outer" for comparison, then normalize to standard format
    how_normalized = how.lower().replace("_", "").replace("outer", "")
    valid_join_types = ["inner", "left", "leftanti", "full"]
    valid_join_types_normalized = {
        "inner": "inner",
        "left": "left_outer" if "outer" in how.lower() else "left",
        "leftanti": "left_anti",
        "full": "full_outer",
    }
    if how_normalized not in valid_join_types:
        valid_types_str = (
            "inner, left (or left_outer), full_outer (or full), left_anti (or leftanti)"
        )
        raise ValueError(
            f"Invalid join type '{how}'. Must be one of: {valid_types_str}. "
            f"Underscores are optional."
        )
    how = valid_join_types_normalized[how_normalized]

    # Validate that look_back_time and max_waiting_time are non-negative
    if look_back_time is None or look_back_time < 0:
        raise ValueError("look_back_time must be a non-negative integer (>= 0)")
    if max_waiting_time is None or max_waiting_time < 0:
        raise ValueError("max_waiting_time must be a non-negative integer (>= 0)")
    # if no end of output window is specified, we take today as end date because the inc_col value cannot be in the future.
    if output_window_end is None:
        output_window_end = DateTime.now()
        log.debug(
            "Output window end not specified; defaulting to today %s",
            output_window_end,
        )

    settings = other_settings or IncJoinSettings()

    if join_cols:
        join_cols_list = (
            [join_cols] if isinstance(join_cols, str) else [str(c) for c in join_cols]
        )
    else:
        join_cols_list = []

    if not join_cols_list and join_cond is None:
        raise ValueError("Either join_cols or join_cond must be provided.")

    if join_cond is not None and not isinstance(join_cond, (str, Column)):
        raise TypeError("join_cond must be a SQL string or a pyspark.sql.Column")

    if log.isEnabledFor(logging.DEBUG):
        join_cond_repr = None
        if join_cond is not None:
            join_cond_repr = join_cond if isinstance(join_cond, str) else str(join_cond)
        log.debug(
            "inc_join parameters | how=%s join_cols=%s join_cond=%s look_back=%s max_wait=%s "
            "output_window=(%s, %s) include_waiting=%s inc_col=%s aliases=(%s,%s) time_uom=%s "
            "enforce_sliding_join_window=%s",
            how,
            join_cols_list if join_cols_list else None,
            join_cond_repr,
            look_back_time,
            max_waiting_time,
            output_window_start,
            output_window_end,
            settings.include_waiting,
            settings.inc_col_name,
            settings.alias_a,
            settings.alias_b,
            settings.time_uom,
            settings.enforce_sliding_join_window,
        )

    output_columns = [
        token.strip()
        for token in (settings.output_select or "").split(",")
        if token.strip()
    ]

    return (
        settings,
        join_cols_list,
        output_window_end,
        join_cond,
        output_columns,
        how,
    )


def rename_columns(df: DataFrame, rename_map: Dict[str, str]) -> DataFrame:
    """Rename columns in df according to rename_map, checking for collisions."""
    for old_name, new_name in rename_map.items():
        if new_name in df.columns:
            raise ValueError(
                f"Column rename collision: Cannot rename '{old_name}' to '{new_name}' because "
                f"'{new_name}' already exists. Adjust aliases or rename columns before calling inc_join."
            )

    result = df
    for old_name, new_name in rename_map.items():
        result = result.withColumnRenamed(old_name, new_name)
    return result


def inc_join(
    df_a: DataFrame,
    df_b: DataFrame,
    how: str = "left",
    join_cols: Union[str, list] = None,
    join_cond: Optional[Union[str, Column]] = None,
    # define the sliding join window:
    look_back_time: int = 0,  # default is 0 days
    max_waiting_time: int = 0,  # default is 0 days
    # define the output window:
    output_window_start: Optional[Union[DateTime, Date]] = None,
    output_window_end: Optional[Union[DateTime, Date]] = None,
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
    `sliding_join_window(df_a) = df_a.[inc_col_name] - look_back_time till df_a.[inc_col_name] + max_waiting_time`

    The output window defines the interval for which we want to generate the output. Records
    in the output will have their [inc_col_name] contained within the output window.

    Args:
        df_a (DataFrame): The first dataset (typically the primary dataset).
        df_b (DataFrame): The second dataset (typically the secondary dataset to join with).
        how (str, optional): Type of join. Must be one of: 'inner', 'left' (or 'left_outer'),
                             'full_outer' (or 'full'), 'left_anti' (or 'leftanti').
                             Underscores are optional. Defaults to 'left'.

        join_cols (Union[str, list], optional): Column(s) to join on. Should exist in both
                                                 datasets. Either join_cols or join_cond must be provided.
        join_cond (Optional[Union[str, Column]], optional): Extra join condition as a SQL expression string
                                             or PySpark Column. Use aliases from IncJoinSettings (default 'A' and 'B')
                                             to refer to df_a and df_b columns.
        look_back_time (int): Look-back interval in days to include late arrivals from df_b.
                             This defines how far back in time to look for matches
                             when df_b arrives before df_a. Defaults to 0.
        max_waiting_time (int): Maximum waiting time in days to allow late arrivals from df_b.
                                This defines how long to wait for matches when df_b
                                arrives after df_a. Defaults to 0.
        output_window_start (Optional[Union[datetime.datetime, datetime.date]]): Start datetime/date of the output window.
                                                               Records in the output will have
                                                               [inc_col_name] >= output_window_start.
                                                               Defaults to None (no lower bound).
        output_window_end (Optional[Union[datetime.datetime, datetime.date]]): End datetime/date of the output window.
                                                             Records in the output will have
                                                             [inc_col_name] <= output_window_end.
                                                             Defaults to None, which will use today's datetime
                                                             (datetime.datetime.now()) as the upper bound.
        other_settings (Optional[IncJoinSettings]): An IncJoinSettings object containing advanced
                                                     join options:
            - alias_a (str): Alias for df_a columns if conflicts occur. Defaults to 'A'.
            - alias_b (str): Alias for df_b columns if conflicts occur. Defaults to 'B'.
            - include_waiting (bool): If True, include unmatched rows from df_a in the output
                                      (waiting/timed out records). Defaults to False.
            - inc_col_name (str): Incremental column name. Column that identifies each increment.
                                   Should be sequential and of date or datetime type.
                                   (Numbers are not supported at this time). Defaults to 'RecDate'.
                                   Should exist in both datasets.
            - output_select (str): Comma separated list of output columns.
                                   Defaults to
                                   "join_cols, inc_col, df_a_cols, df_b_cols, DiffArrivalTime, WaitingTime, JoinType".

    Returns:
        DataFrame: The result of the incremental join, including:
            - Join columns (from join_cols): coalesced values across df_a and df_b, restored under the
              original column names.
            - inc_col_name: Maximum of df_a and df_b incremental column values when matched, or
              df_a.[inc_col_name] + max_waiting_time if unmatched (timed out). Always contained
              within the output window.
            - All columns from df_a (with alias suffix if conflicts exist).
            - All columns from df_b (with alias suffix if conflicts exist).
            - DiffArrivalTime: Difference in days between df_b and df_a incremental column values:
                - 0 if both arrive at the same time.
                - >0 if df_b is late (B.[inc_col_name] > A.[inc_col_name]).
                - <0 if df_b is early or df_a is late (B.[inc_col_name] < A.[inc_col_name]).
                - None if df_a is unmatched (timed out).
            - WaitingTime: For unmatched df_a records, the number of days waited before timing out.
              Computed as the minimum of max_waiting_time and the days between df_a.[inc_col_name] and
              output_window_end. Null for matched records.
            - JoinType: Short string describing which join scenario applies to this record:
                - "same_time": A and B arrived at the same time (delta_arrival_time == 0).
                - "a_late": A arrived later than B (delta_arrival_time < 0).
                - "b_late": B arrived later than A (delta_arrival_time > 0).
                - "a_timed_out": No match found in df_b after max_waiting_time (B.[inc_col_name] is None).
                - "a_waiting": No match found in df_b yet, but WaitingTime < max_waiting_time (only included when include_waiting=True).
                - "not_matched": Occurs primarily in full_outer joins when df_b has unmatched records
                  (no corresponding df_a record), resulting in A.[inc_col_name] being None and
                  DiffArrivalTime being None. Rarely occurs in other join types.
    """
    # step 1: validate the parameters
    (
        settings,
        join_cols_list,
        output_window_end,
        join_cond,
        output_columns,
        how,
    ) = check_inc_join_params(
        join_cols=join_cols,
        join_cond=join_cond,
        look_back_time=look_back_time,
        max_waiting_time=max_waiting_time,
        output_window_start=output_window_start,
        output_window_end=output_window_end,
        other_settings=other_settings,
        how=how,
    )

    # step 2: make sure that column names are unique by adding the dataframe alias as a suffix to the common column names.
    # join_cols and inc_col_name should be in the common columns.
    required_common_cols = {settings.inc_col_name}
    required_common_cols.update(join_cols_list)
    common_cols = set(df_a.columns) & set(df_b.columns)
    missing_common = required_common_cols - common_cols
    if missing_common:
        raise ValueError(
            "join_cols and inc_col_name must be present in both dataframes. "
            f"Missing columns: {sorted(missing_common)}"
        )
    log.debug(f"Common columns to rename: {common_cols}")

    rename_map_a = {col: f"{col}_{settings.alias_a}" for col in common_cols}
    rename_map_b = {col: f"{col}_{settings.alias_b}" for col in common_cols}

    a = rename_columns(df_a, rename_map_a).alias(settings.alias_a)
    b = rename_columns(df_b, rename_map_b).alias(settings.alias_b)
    a_cols = a.columns
    b_cols = b.columns
    inc_col_a_name = f"{settings.inc_col_name}_{settings.alias_a}"
    inc_col_b_name = f"{settings.inc_col_name}_{settings.alias_b}"
    inc_col_a = a[inc_col_a_name]
    inc_col_b = b[inc_col_b_name]

    # step 3: Truncate datetime to date when time_uom is 'day' for proper day-based comparisons
    if settings.time_uom == "day":
        a_col_type = a.schema[inc_col_a_name].dataType
        b_col_type = b.schema[inc_col_b_name].dataType

        if isinstance(a_col_type, TimestampType):
            inc_col_a = F.to_date(inc_col_a)
        if isinstance(b_col_type, TimestampType):
            inc_col_b = F.to_date(inc_col_b)

    # step 4: Apply output window filters
    # Convert datetime objects to date columns respecting session timezone
    # When datetime objects are passed, Spark interprets them in the session timezone
    # We use to_date() on timestamp literals to ensure consistent date comparison
    def to_date_col(dt):
        """Convert datetime to Spark date column respecting session timezone."""
        if dt is None:
            return None
        # Convert datetime to timestamp literal, then to date (respects session timezone)
        return F.to_date(F.lit(dt))

    filter_a = None
    filter_b = None
    if output_window_start is not None:
        # ow = output window
        # A is element of ow left extended with max_waiting_time
        max_wait_extended_start = output_window_start - datetime.timedelta(
            days=max_waiting_time
        )
        filter_a = inc_col_a >= to_date_col(max_wait_extended_start)

        # B is element of ow left extended with look_back_time ( for A is late scenario)
        # B is element of ow left extended with look_back_time + max_waiting_time( for A is timed out scenario)
        # we need to take the sum of max_waiting_time and look_back_time because at ow-max_waiting_time
        # we need to include the timed out records. In order to determine them we need to check if there is a
        # match in B within the look_back_time.
        lb_extended_start = (
            output_window_start
            - datetime.timedelta(days=look_back_time)
            - datetime.timedelta(days=max_waiting_time)
        )
        filter_b = inc_col_b >= to_date_col(lb_extended_start)

    # Always apply upper bound filter
    end_filter_a = inc_col_a <= to_date_col(output_window_end)
    end_filter_b = inc_col_b <= to_date_col(output_window_end)

    if filter_a is not None:
        filter_a = filter_a & end_filter_a
    else:
        filter_a = end_filter_a

    if filter_b is not None:
        filter_b = filter_b & end_filter_b
    else:
        filter_b = end_filter_b

    log.debug(f"Applying filter on df_a: {filter_a}")
    a = a.filter(filter_a)
    log.debug(f"Applying filter on df_b: {filter_b}")
    b = b.filter(filter_b)

    # step 5: Build join expressions on join_cols and join_cond
    # it is already checked that join_cols are present in both dataframes.
    join_exprs = []
    if join_cols_list:
        for c in join_cols_list:
            join_col_a = f"{c}_{settings.alias_a}"
            join_col_b = f"{c}_{settings.alias_b}"
            expr = a[join_col_a] == b[join_col_b]
            join_exprs.append(expr)
    else:
        log.debug("No join columns supplied; relying on join_cond for join logic")
    if join_cond is not None:
        log.debug("Using custom join condition")
        if isinstance(join_cond, str):
            join_exprs.append(F.expr(join_cond))
        elif isinstance(join_cond, Column):
            join_exprs.append(join_cond)
        else:
            raise TypeError("join_cond must be a SQL string or a pyspark.sql.Column")

    log.debug(f"Join expressions: {join_exprs if join_exprs else 'None'}")

    # step 6: Apply sliding join window (optional):
    if settings.enforce_sliding_join_window:
        log.debug(f"Sliding window: [-{look_back_time}..+{max_waiting_time}]")
        # Lower bound: B.[inc_col_name] >= A.[inc_col_name] - look_back_time
        join_exprs.append(inc_col_b >= F.date_sub(inc_col_a, look_back_time))
        # Upper bound: B.[inc_col_name] <= A.[inc_col_name] + max_waiting_time
        join_exprs.append(inc_col_b <= F.date_add(inc_col_a, max_waiting_time))

    # step 7: Invoke the join
    final_join_condition = reduce(and_, join_exprs)
    result = a.join(b, final_join_condition, how)

    # For left_anti and left_semi joins, B columns are not in the result
    # Add them as null columns so downstream logic can work
    b_schema = {field.name: field.dataType for field in b.schema}
    for col_name in b_cols:
        if col_name not in result.columns:
            result = result.withColumn(col_name, F.lit(None).cast(b_schema[col_name]))

    # Get column references after ensuring they exist
    inc_col_a = F.col(inc_col_a_name)
    inc_col_b = F.col(inc_col_b_name)

    # step 8: Restore one of the original join columns, give preference to df_a.join_col.
    if join_cols_list:
        for c in join_cols_list:
            join_col_a = f"{c}_{settings.alias_a}"
            join_col_b = f"{c}_{settings.alias_b}"
            result = result.withColumn(
                c,
                F.coalesce(
                    F.col(join_col_a),
                    F.col(join_col_b),
                ),
            )

    # step 9: Calculate metric DiffArrivalTime: B.[inc_col_name] - A.[inc_col_name] in days
    # Skip for left_anti joins as there are no matches
    if how != "left_anti":
        result = result.withColumn(
            "DiffArrivalTime",
            F.when(inc_col_b.isNotNull(), F.datediff(inc_col_b, inc_col_a)).otherwise(
                None
            ),
        )
    # step 10: Calculate metric waiting time for records in A that do not have a match in B
    # Convert datetime to date column respecting session timezone
    end_dt_col = to_date_col(output_window_end)
    waiting_time = F.least(
        F.lit(max_waiting_time),
        F.greatest(F.lit(0), F.datediff(end_dt_col, inc_col_a)),
    )
    result = result.withColumn("WaitingTime", F.when(inc_col_b.isNull(), waiting_time))

    # step 11: define join_type
    # Skip for left_anti joins as there are no matches
    if how != "left_anti":
        timed_out_condition = inc_col_b.isNull() & (
            F.col("WaitingTime") == F.lit(max_waiting_time)
        )
        waiting_condition = (
            inc_col_b.isNull()
            & (
                (F.col("WaitingTime") < F.lit(max_waiting_time))
                & (
                    F.lit(max_waiting_time) > F.lit(0)
                )  # if max_waiting_time is 0 then records are timed out immediately ( no waiting).
            )
        )
        join_type = (
            F.when(timed_out_condition, F.lit("a_timed_out"))
            .when(waiting_condition, F.lit("a_waiting"))
            .when(F.col("DiffArrivalTime") == 0, F.lit("same_time"))
            .when(F.col("DiffArrivalTime") < 0, F.lit("a_late"))
            .when(F.col("DiffArrivalTime") > 0, F.lit("b_late"))
            .otherwise(F.lit("not_matched"))
        )
        result = result.withColumn("JoinType", join_type)

    # step 12: filter out waiting records by default
    # By default, waiting records are not included in the output,
    # because the output contains only matched records or timed out records.
    # but you can override this by behaviour via the include_waiting setting.
    if not settings.include_waiting:
        if how == "left_anti":
            # For left_anti joins, all records are unmatched, so filter based on WaitingTime
            # Only include timed out records (WaitingTime == max_waiting_time)
            filter_expr = F.col("WaitingTime") == F.lit(max_waiting_time)
        else:
            filter_expr = F.col("WaitingTime").isNull() | (
                F.col("JoinType") == F.lit("a_timed_out")
            )
        log.debug(
            "Filter waiting records. E.g. having a waiting time and not being timed out."
        )
        result = result.filter(filter_expr)

    # step 13: Compute final inc_col_name
    # When matched: max(A.[inc_col_name], B.[inc_col_name])
    # When timed out: A.[inc_col_name] + max_waiting_time
    result = result.withColumn(
        settings.inc_col_name,
        F.when(inc_col_b.isNotNull(), F.greatest(inc_col_a, inc_col_b)).otherwise(
            F.date_add(inc_col_a, F.col("WaitingTime"))
        ),
    )
    log.debug(
        "Calculating %s as greatest(%s, %s) when matched, otherwise date_add(%s, WaitingTime)",
        settings.inc_col_name,
        inc_col_a_name,
        inc_col_b_name,
        inc_col_a_name,
    )

    # step 14: filter on output window, only keep records that are within the output window
    # This will remove the records in the lookback + waiting period.
    if output_window_start is not None:
        result = result.filter(
            (F.col(settings.inc_col_name) >= to_date_col(output_window_start))
            & (F.col(settings.inc_col_name) <= to_date_col(output_window_end))
        )

    # step 15: select output columns
    result_cols = []
    for output_col in output_columns:
        key = output_col.lower()
        if key == "join_cols":
            for c in join_cols_list:
                result_cols.append(c)
        elif key == "inc_col":
            result_cols.append(settings.inc_col_name)
        elif key == "df_a_cols":
            for c in a_cols:
                result_cols.append(c)
        elif key == "df_b_cols":
            # Skip df_b_cols for left_anti joins as there are no matches
            if how != "left_anti":
                for c in b_cols:
                    result_cols.append(c)
        else:
            # Skip DiffArrivalTime and JoinType for left_anti joins
            if how == "left_anti" and output_col in ["DiffArrivalTime", "JoinType"]:
                continue
            result_cols.append(output_col)
    result = result.select(*result_cols)

    log.debug("inc_join completed")
    return result
