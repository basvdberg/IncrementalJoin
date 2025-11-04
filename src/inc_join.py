from pyspark.sql import DataFrame

def inc_join(
    df_a: DataFrame,
    df_b: DataFrame,
    how: str = 'inner',
    join_cols: str = None,
    join_condition: str = None,
    alias_a: string = 'A',
    alias_b: string = 'B',
    include_waiting: bool = False,
    timestamp_col_name: str = 'RecordDT'
) -> DataFrame:
    """
    Perform an incremental join between two datasets.

    Args:
        df_a (DataFrame): The first dataset.
        df_b (DataFrame): The second dataset.
        how (str, optional): Type of join. Must be one of: 'inner', 'cross', 'outer', 'full', 'fullouter', 'full_outer',
                             'left', 'leftouter', 'left_outer', 'right', 'rightouter', 'right_outer', 'semi',
                             'leftsemi', 'left_semi', 'anti', 'leftanti', 'left_anti'. Defaults to 'inner'.
        join_cols (str, optional): Column name(s) to join on. Should exist in both datasets.  
        join_condition (str, optional): Extra join condition. Use aliases `a` and `b` to refer to `df_a` and `df_b`.
        include_waiting (bool, optional): If True, include waiting records on A.RecordDT that don't match with B. Defaults to False.
        timestamp_col_name (str, optional): Name of the incremental timestamp column. Defaults to 'RecordDT'.

    Returns:
        DataFrame: The result of the incremental join. Columns: 
            - columns in A *
            - columns in B * 
            * if a columns exists in both A and B then the column is made unique by suffixing an underscore and (alias_a or alias_b).
            - timestamp_col_name ( e.g. RecordDT). This wil be max(A.timestamp_col_name, B.timestamp_col_name) when there is a match or 
              A.timestamp_col_name + waiting_interval when A is timed out (no match).  
            - DiffArrivalTimeDays: this gives B.RecordDT - A.RecordDT. This can be:a
                - 0. Both A and B arrive on the same time.
                - >0 B is late. 
                - <0 B is early ( or A is late).
                - None. A is timed out.   
    """
    
    return result