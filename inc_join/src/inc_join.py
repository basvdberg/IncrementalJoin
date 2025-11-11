import datetime
import logging
from dataclasses import dataclass
from functools import reduce
from operator import and_
from typing import Optional, Union

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
                             Defaults to 'RecordDT'. Should exist in both datasets.
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
            - inc_col_a / inc_col_b: the incremental columns originating from df_a / df_b.
            - DiffArrivalTime / WaitingTime / JoinType: the derived columns produced by inc_join.
            You can also provide explicit column names. Defaults to
            "join_cols, inc_col, df_a_cols, df_b_cols, inc_col_a, inc_col_b, DiffArrivalTime, WaitingTime, JoinType".
    """

    alias_a: str = "A"
    alias_b: str = "B"
    include_waiting: bool = False
    inc_col_name: str = "RecordDT"
    time_uom: str = "day"
    enforce_sliding_join_window: bool = True
    output_select: str = (
        "join_cols, inc_col, df_a_cols, df_b_cols, inc_col_a, inc_col_b, "
        "DiffArrivalTime, WaitingTime, JoinType"
    )

    def __post_init__(self) -> None:
        if self.time_uom not in ["day"]:
            raise ValueError(
                "Invalid time unit of measure. Currently only 'day' uom is supported."
            )


def inc_join(
    df_a: DataFrame,
    df_b: DataFrame,
    how: str = "inner",
    join_cols: Union[str, list] = None,
    join_cond: Optional[Union[str, Column]] = None,
    # define the sliding join window:
    look_back_time: int = 0,  # default is 0 days
    max_waiting_time: int = 0,  # default is 0 days
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
    `sliding_join_window(df_a) = df_a.[inc_col_name] - look_back_time till df_a.[inc_col_name] + max_waiting_time`

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
        max_waiting_time (int): Maximum waiting time in days to allow late arrivals from df_b.
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
            - output_select (str): Comma separated tokens that define the column order of the output.
                                   Supports the same tokens as IncJoinSettings.output_select. Defaults to
                                   "join_cols, inc_col, df_a_cols, df_b_cols, inc_col_a, inc_col_b, DiffArrivalTime, "
                                   "WaitingTime, JoinType".

    Returns:
        DataFrame: The result of the incremental join, including:
            - All columns from df_a (with alias suffix if conflicts exist).
            - All columns from df_b (with alias suffix if conflicts exist).
            - Join columns (from join_cols): coalesced values across df_a and df_b, restored under the
              original column names.
            - inc_col_name: Maximum of df_a and df_b incremental column values when matched, or
              df_a.[inc_col_name] + max_waiting_time if unmatched (timed out). Always contained
              within the output window.
            - DiffArrivalTime: Difference in days between df_b and df_a incremental column values:
                - 0 if both arrive at the same time.
                - >0 if df_b is late (B.[inc_col_name] > A.[inc_col_name]).
                - <0 if df_b is early or df_a is late (B.[inc_col_name] < A.[inc_col_name]).
                - None if df_a is unmatched (timed out).
            - WaitingTime: For unmatched df_a records, the number of days waited before timing out.
              Computed as the minimum of max_waiting_time and the days between df_a.[inc_col_name] and
              output_window_end_dt. Null for matched records.
            - JoinType: String describing which numbered scenario applied to each record:
                - "1_same_time"
                - "2_a_late"
                - "3_b_late"
                - "4_a_timed_out" (also assigned when WaitingTime equals max_waiting_time)
                - "5_a_waiting" (assigned when no match found but WaitingTime < max_waiting_time)

    Join Scenarios:
        The function handles five join scenarios:
        1. Same time: A and B arrived at the same time (delta_arrival_time == 0).
        2. A is late: A arrived later than B (delta_arrival_time < 0).
        3. B is late: B arrived later than A (delta_arrival_time > 0).
        4. A is timed out: No match found in df_b after max_waiting_time (B.[inc_col_name] is None).
        5. A is waiting: No match found in df_b yet, but WaitingTime < max_waiting_time (only included when include_waiting=True).
    """
    # Validate that look_back_time and max_waiting_time are non-negative
    if look_back_time is None or look_back_time < 0:
        raise ValueError("look_back_time must be a non-negative integer (>= 0)")
    if max_waiting_time is None or max_waiting_time < 0:
        raise ValueError("max_waiting_time must be a non-negative integer (>= 0)")
    # if no end of output windows is specified, we filter up untill today ( no future data)
    if output_window_end_dt is None:
        output_window_end_dt = datetime.datetime.now()
        log.debug(
            "Output window end not specified; defaulting to current timestamp %s",
            output_window_end_dt,
        )
    if other_settings is None:
        settings = IncJoinSettings()  # use defaults
    else:
        settings = other_settings

    join_cols_list: list[str] = []
    if join_cols:
        join_cols_list = [join_cols] if isinstance(join_cols, str) else list(join_cols)

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
            output_window_start_dt,
            output_window_end_dt,
            settings.include_waiting,
            settings.inc_col_name,
            settings.alias_a,
            settings.alias_b,
            settings.time_uom,
            settings.enforce_sliding_join_window,
        )

    common_cols = set(df_a.columns) & set(df_b.columns)
    if common_cols:
        log.debug(f"Common columns to rename: {common_cols}")
    else:
        log.debug("No common columns detected between df_a and df_b")

    # Helper function to rename columns with collision check
    def rename_columns(df, rename_map, df_name):
        """Rename columns in df according to rename_map, checking for collisions."""
        for old_name, new_name in rename_map.items():
            if new_name in df.columns:
                raise ValueError(
                    f"Column rename collision: Cannot rename '{old_name}' to '{new_name}' because "
                    f"'{new_name}' already exists in {df_name}. Adjust aliases or rename columns before calling inc_join."
                )

        result = df
        for old_name, new_name in rename_map.items():
            result = result.withColumnRenamed(old_name, new_name)
        return result

    # Create renaming maps and perform renames
    rename_map_a = {col: f"{col}_{settings.alias_a}" for col in common_cols}
    rename_map_b = {col: f"{col}_{settings.alias_b}" for col in common_cols}

    df_a_renamed = rename_columns(df_a, rename_map_a, "df_a")
    df_b_renamed = rename_columns(df_b, rename_map_b, "df_b")

    # Create column name mappings for later use (all columns with renamed ones mapped)
    a_col_names = {col: rename_map_a.get(col, col) for col in df_a.columns}
    b_col_names = {col: rename_map_b.get(col, col) for col in df_b.columns}

    a = df_a_renamed.alias(settings.alias_a)
    b = df_b_renamed.alias(settings.alias_b)

    # Apply output window filters on df_a
    if output_window_start_dt is not None:
        # ow = output window
        # A is element of ow left extended with max_waiting_time
        max_wait_extended_start = (
            output_window_start_dt
            - datetime.timedelta(days=max_waiting_time)
        )
        filter_a = a[a_col_names[settings.inc_col_name]] >= F.lit(
            max_wait_extended_start
        )
        filter_a = filter_a & (
            a[a_col_names[settings.inc_col_name]] <= F.lit(output_window_end_dt)
        )

        # B is element of ow left extended with look_back_time ( for A is late scenario)
        # B is element of ow left extended with look_back_time + max_waiting_time( for A is timed out scenario)
        # we need to take the sum of max_waiting_time and look_back_time because at ow-max_waiting_time
        # we need to include the timed out records. In order to determine them we need to check if there is a
        # match in B within the look_back_time.
        lb_extended_start = (
            output_window_start_dt 
            - datetime.timedelta(days=look_back_time) 
            - datetime.timedelta(days=max_waiting_time)
        )
        filter_b = b[b_col_names[settings.inc_col_name]] >= F.lit(lb_extended_start)
        filter_b = filter_b & (
            b[b_col_names[settings.inc_col_name]] <= F.lit(output_window_end_dt)
        )

        log.debug(
            "Applying filter on df_a: %s BETWEEN %s AND %s",
            a_col_names[settings.inc_col_name],
            max_wait_extended_start,
            output_window_end_dt,
        )
        a = a.filter(filter_a)
        log.debug(
            "Applying filter on df_b: %s BETWEEN %s AND %s",
            b_col_names[settings.inc_col_name],
            lb_extended_start,
            output_window_end_dt,
        )
        b = b.filter(filter_b)

    # Build join expressions on join_cols and join_cond
    join_exprs = []
    join_col_exprs = []
    if join_cols_list:
        log.debug(f"Join columns: {join_cols_list}")
        for c in join_cols_list:
            if c not in a_col_names or c not in b_col_names:
                raise ValueError(f"Join column '{c}' must exist in both dataframes.")
            expr = a[a_col_names[c]] == b[b_col_names[c]]
            join_exprs.append(expr)
            join_col_exprs.append(expr)
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

    # Apply sliding join window (optional):
    # If enforced, B.[inc_col_name] must be in [A.[inc_col_name] - look_back_time, A.[inc_col_name] + max_waiting_time]
    inc_col_a_raw = a[a_col_names[settings.inc_col_name]]
    inc_col_b_raw = b[b_col_names[settings.inc_col_name]]

    inc_col_a = inc_col_a_raw
    inc_col_b = inc_col_b_raw

    # Truncate datetime to date when time_uom is 'day' for proper day-based comparisons
    if settings.time_uom == "day":
        # Check if columns are timestamp/datetime types by inspecting schema
        # F.to_date() is safe to use on both date and timestamp types
        a_col_type = df_a.schema[settings.inc_col_name].dataType
        b_col_type = df_b.schema[settings.inc_col_name].dataType

        if isinstance(a_col_type, TimestampType):
            inc_col_a = F.to_date(inc_col_a)
        if isinstance(b_col_type, TimestampType):
            inc_col_b = F.to_date(inc_col_b)

    if settings.enforce_sliding_join_window:
        log.debug(f"Sliding window: [-{look_back_time}..+{max_waiting_time}]")
        # Lower bound: B.[inc_col_name] >= A.[inc_col_name] - look_back_time
        join_exprs.append(inc_col_b >= F.date_sub(inc_col_a, look_back_time))
        # Upper bound: B.[inc_col_name] <= A.[inc_col_name] + max_waiting_time
        join_exprs.append(inc_col_b <= F.date_add(inc_col_a, max_waiting_time))

    if join_col_exprs:
        log.debug("Join expressions (join_cols): %s", join_col_exprs)
    else:
        log.debug("Join expressions (join_cols): None")

    if not join_exprs:
        raise ValueError("Either join_cols or join_cond must be provided.")

    final_join_condition = reduce(and_, join_exprs)
    result = a.join(b, final_join_condition, how)

    if join_cols_list:
        for c in join_cols_list:
            result = result.withColumn(
                c, F.coalesce(F.col(a_col_names[c]), F.col(b_col_names[c]))
            )

    # Compute DiffArrivalTime: B.[inc_col_name] - A.[inc_col_name] in days
    result = result.withColumn(
        "DiffArrivalTime",
        F.when(inc_col_b.isNotNull(), F.datediff(inc_col_b, inc_col_a)).otherwise(None),
    )
    # calculate the waiting time for records in A that do not have a match in B
    waiting_time = F.least(
        F.lit(max_waiting_time),
        F.greatest(F.lit(0), F.datediff(F.lit(output_window_end_dt), inc_col_a)),
    )
    result = result.withColumn("WaitingTime", F.when(inc_col_b.isNull(), waiting_time))

    # classify each record into the numbered join scenarios
    timed_out_condition = inc_col_b.isNull() & (
        F.col("WaitingTime") == F.lit(max_waiting_time)
    )
    waiting_condition = inc_col_b.isNull() & (
        F.col("WaitingTime") < F.lit(max_waiting_time)
    )
    scenario_col = (
        F.when(timed_out_condition, F.lit(4))
        .when(F.col("DiffArrivalTime") == 0, F.lit(1))
        .when(F.col("DiffArrivalTime") < 0, F.lit(2))
        .when(F.col("DiffArrivalTime") > 0, F.lit(3))
        .when(waiting_condition, F.lit(5))
        .otherwise(F.lit(None))
    ).cast("smallint")
    result = result.withColumn("JoinType", scenario_col)

    # By default, waiting records are not included in the output,
    # because the output contains only matched records or timed out records.
    # but you can override this by behaviour via the include_waiting setting.
    if not settings.include_waiting:
        filter_expr = F.col("WaitingTime").isNull() | (F.col("JoinType") == F.lit(4))
        log.debug(
            "Filter waiting records. E.g. having a waiting time and not being timed out."
        )
        result = result.filter(filter_expr)

    # Compute final inc_col_name
    # When matched: max(A.[inc_col_name], B.[inc_col_name])
    # When timed out: A.[inc_col_name] + max_waiting_time
    result = result.withColumn(
        settings.inc_col_name,
        F.when(
            inc_col_b.isNotNull(), F.greatest(inc_col_a_raw, inc_col_b_raw)
        ).otherwise(F.date_add(inc_col_a_raw, F.col("WaitingTime"))),
    )
    log.debug(
        "Calculating %s as greatest(%s, %s) when matched, otherwise date_add(%s, WaitingTime)",
        settings.inc_col_name,
        a_col_names[settings.inc_col_name],
        b_col_names[settings.inc_col_name],
        a_col_names[settings.inc_col_name],
    )

    if output_window_start_dt is not None:
        result = result.filter(
            (F.col(settings.inc_col_name) >= F.lit(output_window_start_dt))
            & (F.col(settings.inc_col_name) <= F.lit(output_window_end_dt))
        )

    def _resolve_output_columns() -> list[str]:
        """
        Resolve the settings.output_select tokens into concrete column names.
        """
        tokens = [
            token.strip()
            for token in (settings.output_select or "").split(",")
            if token.strip()
        ]
        if not tokens:
            return result.columns

        resolved: list[str] = []
        existing_columns = set(result.columns)

        def append_column(col_name: str) -> None:
            if col_name not in existing_columns:
                log.debug("Skipping missing column '%s' from output_select", col_name)
                return
            if col_name not in resolved:
                resolved.append(col_name)

        for token in tokens:
            key = token.lower()
            if key == "join_cols":
                for c in join_cols_list:
                    append_column(c)
            elif key == "inc_col":
                append_column(settings.inc_col_name)
            elif key == "df_a_cols":
                for original_col in df_a.columns:
                    append_column(a_col_names[original_col])
            elif key == "df_b_cols":
                for original_col in df_b.columns:
                    append_column(b_col_names[original_col])
            elif key == "inc_col_a":
                append_column(a_col_names[settings.inc_col_name])
            elif key == "inc_col_b":
                append_column(b_col_names[settings.inc_col_name])
            elif key in {"diffarrivaltimedays", "diffarrivaltime"}:
                append_column("DiffArrivalTime")
            elif key == "waitingtime":
                append_column("WaitingTime")
            elif key in {"incjoinscenario", "jointype"}:
                append_column("JoinType")
            else:
                append_column(token)

        return resolved if resolved else result.columns

    output_columns = _resolve_output_columns()
    result = result.select(*output_columns)

    log.debug("inc_join completed")
    return result
