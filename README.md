- [Dependencies](#dependencies)
- [Summary](#summary)
- [Installation and Usage](#installation-and-usage)
  - [Installation](#installation)
  - [Quick Start Example](#quick-start-example)
- [Incremental refresh](#incremental-refresh)
- [Incremental join](#incremental-join)
- [Example](#example)
  - [A](#a)
  - [B](#b)
  - [Arrival time of A versus B](#arrival-time-of-a-versus-b)
    - [Observations:](#observations)
  - [Sliding join window](#sliding-join-window)
    - [Look back time](#look-back-time)
    - [Maximum waiting time](#maximum-waiting-time)
    - [Timed out records](#timed-out-records)
    - [Output window](#output-window)
- [Implementation](#implementation)
  - [Join scenarios inc\_join(df\_a, df\_b)](#join-scenarios-inc_joindf_a-df_b)
    - [1. Same time](#1-same-time)
    - [2. A is late](#2-a-is-late)
    - [3. B is late](#3-b-is-late)
    - [4. A is timed out](#4-a-is-timed-out)
- [Improvements](#improvements)

# Dependencies 
- Python >= 3.9 (it might run on earlier versions, but we did not test that)
- PySpark >= 3.0.0

# Summary

This library implements an incremental join function to join 2 (usually large) incrementally refreshed tables, taking into account the refresh timestamp of each table and the fact that data might arrive late. 

The next section is a quick start and installation guide. After that we will describe how it works in more detail. 

# Installation and Usage

## Installation

Install the library using pip:

```bash
pip install inc-join
```

This will automatically install PySpark as a dependency (requires PySpark >= 3.0.0).

## Quick Start Example

See [`scripts/usage_example1.py`](scripts/usage_example1.py) for a complete working example.

# Incremental refresh

Suppose you have two big datasets that are incrementally refreshed. This means that every day we add or change only the new data and mark this with a unique timestamp. Using this timestamp, downstream applications can pick up these changes and there is no need for an expensive full table refresh. 

# Incremental join

After loading the data, we want to join the two datasets. This join can become quite complex, because you will have to take into account that data might arrive late, or not at all. There is a tradeoff between completeness and performance here. When tables are small, we usually include all data in the join. This makes our code simple, but also slower. For big data tables this is not an option. 

**Requirements of incremental join function:**
- Performance: Filter A and B into the smallest subsets possible before joining. 
- Consistency: The join results remain consistent regardless of the output window size (daily, monthly, yearly). This ensures that historical data doesn't change when reprocessing with different window sizes, which is crucial for machine learning workflows. This behavior is controlled by the `enforce_sliding_join_window` parameter (default: `True`). When enabled, records unmatched in a daily load will remain unmatched in a yearly historic load, preventing data drift in ML models that depend on previously processed data.  

Moving the join logic to a dedicated function implements the following requirements:
- Efficiency: Users do not have to worry about the complexity of incremental join. Secondly by using this function you know that every incremental join works identical.  
- Maintainability: The complexity of incremental join is removed from your code and thus it makes your code more readable and easier to maintain.  
- Code quality: Prevent user mistakes in the complex joining conditions of incremental join by externalizing and parameterizing this code. 

# Example

To explain the implementation of incremental joining we introduce the following example datasets:

- A: Bank Transactions from the financial system  
- B: Bank Transactions from the SEPA payment engine.

Note that both tables are huge (multiple terabytes).

Let's introduce some example data:
## A
| TrxDT               | CreditDebit | AmountEuro   | AccountName    | TrxId | RecDate<sup>[1](#fn1)</sup> |
| ------------------- | ----------- | ------------ | -------------- | ----- | ---------- |
| 2025-03-06 20:45:19 | Credit      | 700.3000000  | Madame Zsa Zsa | 1     | 2025-03-06 |
| 2025-03-06 12:22:01 | Debit       | 200.0000000  | Madame Zsa Zsa | 2     | 2025-03-06 |
| 2025-03-06 20:59:00 | Debit       | 1110.2000000 | Madame Zsa Zsa | 3     | 2025-03-06 |
| 2025-03-06 23:50:00 | Credit      | 50.0000000   | Madame Zsa Zsa | 4     | 2025-03-07 |
| 2025-03-06 08:00:00 | Credit      | 1500.0000000 | Mr. X          | 5     | 2025-03-07 |
| 2025-03-07 14:45:00 | Debit       | 300.2500000  | Mr. X          | 6     | 2025-03-07 |
| 2025-03-10 09:00:00 | Credit      | 99.9900000   | Mr. X          | 7     | 2025-03-08 |

<sub>1. <a name="fn1"></a>RecDate represents the moment at which the record was stored(recorded) in this dataset. For simplicity we use dates instead of timestamps</sub>

## B
| TrxId | CountryCode | RecDate |
| ----- | ----------- | ---------- |
| 1     | NL          | 2025-03-05 |
| 2     | NL          | 2025-03-04 |
| 3     | NL          | 2025-03-06 |
| 4     | UK          | 2025-03-07 |
| 5     | NL          | 2025-03-12 |
| 6     | NL          | 2025-03-18 |
| 7     | DE          | 2025-03-06 |

## Arrival time of A versus B
The following chart shows the arrival time of A versus the arrival time of B. Arrival time is a synonym of RecDate.   

![datasets](docs/arrival_time_a_b.png)

### Observations:

We will abbreviate a transaction as Trx. 

- There are 7 transactions, each with a unique TrxId
- Trx 3 and 4 arrive on the same day in dataset A and B.
- Trx 1, 2 and 7 have already arrived in B at the time they arrive in A (we say that A is late).
- Trx 5 and 6 arrive late in B (B is late).    

Let's quantify how late B is with respect to A.
| TrxId | RecDate_A | RecDate_B | DiffDays |
| ----- | ------------ | ------------ | -------- |
| 1     | 2025-03-06   | 2025-03-05   | -1        |
| 2     | 2025-03-06   | 2025-03-04   | -2        |
| 3     | 2025-03-06   | 2025-03-06   | 0        |
| 4     | 2025-03-07   | 2025-03-07   | 0        |
| 5     | 2025-03-07   | 2025-03-12   | 5       |
| 6     | 2025-03-07   | 2025-03-18   | 11      |
| 7     | 2025-03-08   | 2025-03-06   | -2        |

## Sliding join window

The sliding join window defines how we filter B when incrementally joining A with B. The sliding join window is defined with respect to dataset A. This example shows that we need to look back at most 2 days and wait 11 days in order to always find a match in B. 

`sliding_join_window(df_a) = df_a.RecDate - look_back_time until df_a.RecDate + max_waiting_time`

We define the sliding join window on df_a and then apply it on df_b (df_b.RecDate must be contained in the sliding_join_window(df_a)).
Note that this window is a sliding window with respect to the RecDate of A. 

### Look back time
How much time should we include in our filter to look back in arrival time of B? Setting look back too high has a negative impact on performance. Setting it too low will result in mismatches in the join. 

### Maximum waiting time
How much time should we wait for the arrival of B. Setting the maximum waiting time too high results in high latencies in the delivery of the data to the consumer. Setting it too low will result in mismatches in the join.  

The effective waiting time that is applied to unmatched records is computed as:  
`WaitingTime = min(max_waiting_time, max(0, output_window_end - A.RecDate))`.

Ideally look back and maximum waiting time are defined on max deltas between df_a.RecDate and df_b.RecDate in the historic data, potentially incremented with some bandwidth for future scenarios.  

![datasets](docs/datasets-1.png)

### Timed out records

The maximum waiting time defines how long we should wait for a matching record in B. When a match is not found after this time, we call the record timed out. Waiting means that the incremental join will not output the record during the waiting interval. At the moment it is timed out, the record will be sent to the output having a timed out status. 

For example, if we would take a waiting interval smaller than 11 then Trx 6 would be timed out at A.RecDate + wait interval. E.g. when the wait interval would be 10, Trx 6 would be timed out at 2025-03-17.

### Output window

The output window defines the interval for which we want to generate the output of the incremental join. Typical values are: 
- daily
- monthly 
- yearly  
- all 
  
But you are free to choose any custom output window as well.  
The output window is not a moving window. It does not slide with the value of RecDate. Typical use for the output window is first for historic loading: monthly, yearly or all (depending on the size of the data and the capacity of your cluster) and after this daily for the daily increments. 

For example: we want to have the output of the inc_join for 2025-03-07 (daily), or 2025-03 (monthly). 

# Implementation

Our implementation is split up into 4 scenarios, each having a different use of the **sliding join window** and **output window**. We assume that time_uom equals day (to keep our documentation simple). 

- `df_output:Dataframe` is the output dataset, where RecDate is always contained in the output window. 
- `A` stands for a record in df_a  
- `B` stands for a record in df_b. 
- `delta_arrival_time:int` is defined as B.RecDate - A.RecDate in days (default uom).
- `WaitingTime:int` (present only when `include_waiting=True`) is defined for unmatched A records and equals the number of days between A.RecDate
  and `output_window_end`, capped by `max_waiting_time`.
- `JoinType:int` contains the numbered scenario code applied to each record (1 = same_time, 2 = a_late, 3 = b_late, 4 = a_timed_out, 5 = a_waiting).
- Join keys (from `join_cols`) are reintroduced as `coalesce(df_a.key, df_b.key)` so downstream consumers
  can rely on the original column names.
- `output_select:str` lets you control which columns the output contains. Use comma separated tokens such as
  `join_cols`, `inc_col`, `df_a_cols`, `df_b_cols`, `inc_col_a`, `inc_col_b`, `DiffArrivalTime`, `WaitingTime` (when available),
  `JoinType`, or explicit column names to tailor the output order.
- `time_uom:string = 'day'` Currently we only implemented unit of measure = 'day'. We could also implement time_uom='hour', or perhaps even 'minute'. 

## Join scenarios inc_join(df_a, df_b)

When joining df_a with df_b we evaluate the join scenario for every record A in df_A and B in df_B. 
Of course the general join conditions should always apply for A and B. (e.g. A.foreign_keys = B.prim_keys)

### 1. Same time

A and B arrived on the same day within the output window. 
- `delta_arrival_time == 0` 
- `A.RecDate is contained in the output window` 
- `B.RecDate is contained in the output window`. This follows from the first 2 conditions. 
- if `enforce_sliding_join_window` then 
  - `B.RecDate is contained in the sliding join window of df_a.RecDate`
- `Output.RecDate = max(df_A.RecDate, df_B.RecDate)`
- `JoinType = 1` (same_time)


### 2. A is late

A arrived later than B. We need to look back to find B (using look back time). 
- `delta_arrival_time < 0` 
- `A.RecDate is contained in the output window` 
- `B.RecDate is contained in the output window extended with -look_back_time` This means that B might have arrived before the start of the output window, or within the output window.  
- if `enforce_sliding_join_window` then 
  - `B.RecDate is contained in the sliding join window of df_a.RecDate`
- `Output.RecDate = df_A.RecDate`
- `JoinType = 2` (a_late)

### 3. B is late

B arrived later than A. 

We cannot extend the filter that is based on output window to the future, because the output window can be set to the latest available increment. **Assumption**: if there is a previous increment, then it will be loaded, or in other words: the data is loaded historically in a sequential order. So we can always extend our filter to the past.  

- `delta_arrival_time > 0` 
- `B.RecDate is contained in the output window` 
- `A.RecDate is contained in the output window extended with -max_waiting_time + 1` So A might have arrived before the beginning of the output window. +1 here is done to exclude the timed out records, which is the next scenario. 
- if `enforce_sliding_join_window` then 
  - `B.RecDate is contained in the sliding join window of df_a.RecDate`
- `Output.RecDate = df_B.RecDate`
- `JoinType = 3` (b_late)

### 4. A is timed out

- `B.RecDate is None (no matching record found in df_B)` 
- `delta_arrival_time is None (because B.RecDate is None)`.   
- There is no match found in the above scenarios
- `A.RecDate is contained in the output window shifted with -max_waiting_time` which means that both start and end date are reduced by max_waiting_time.
- if `enforce_sliding_join_window` then 
  - `B.RecDate is contained in the sliding join window of df_a.RecDate`
- `Output.RecDate = df_A.RecDate + WaitingTime` (when `WaitingTime` is computed, i.e. when `include_waiting=True`)
- `JoinType = 4` (a_timed_out, also assigned whenever `WaitingTime == max_waiting_time`)

# Improvements

1. Support for datetime time windows.   
We decided to limit the first implementation to full dates, because this was sufficient for our use cases, but we could extend this to datetime in order to support datasets that are refreshed multiple times per day. 
