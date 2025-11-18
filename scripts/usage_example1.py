from datetime import date

from pyspark.sql import SparkSession

from inc_join import inc_join

# Initialize Spark session
spark = SparkSession.builder.appName("IncrementalJoinExample").getOrCreate()

# Create example DataFrames
# df_a: Financial transactions with RecDate (record date)
df_a = spark.createDataFrame(
    [
        (1, "Credit", 100.0, date(2025, 3, 6)),
        (2, "Debit", 50.0, date(2025, 3, 7)),
    ],
    ["TrxId", "Type", "Amount", "RecDate"],
)

# df_b: Payment engine data with RecDate
df_b = spark.createDataFrame(
    [
        (1, "NL", date(2025, 3, 5)),  # Arrived 1 day before df_a
        (2, "NL", date(2025, 3, 11)),  # Arrived 4 days after df_a
    ],
    ["TrxId", "Country", "RecDate"],
)

# Perform incremental join
result = inc_join(
    df_a=df_a,
    df_b=df_b,
    how="left",  # Left join (keeps all records from df_a)
    join_cols="TrxId",  # Join on transaction ID
    look_back_time=2,  # Look back 2 days for late arrivals
    max_waiting_time=5,  # Wait up to 5 days for late arrivals
    output_window_start_dt=date(2025, 3, 6),
    output_window_end_dt=date(2025, 3, 6),
)

# Show results
result.show()

# Stop Spark session
spark.stop()
