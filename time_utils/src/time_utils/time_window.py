from pyspark.sql import Column, functions as F
from datetime import date, datetime, timedelta

class TimeWindow:
    """
    Represents a closed time window [start, end]. Closed here means that start and end are included in the window. 

    Methods:
        contains(col_time): Returns a Spark Column expression checking if a column's
                            timestamp lies within the window.
        duration(): Returns the duration of the window as a timedelta.
        shift(delta): Returns a new TimeWindow shifted by a given timedelta.
        expand(before, after): Returns a new TimeWindow expanded by the given timedeltas.
    """


    def __init__(self, start: date | datetime, end: date | datetime):
        """
        Initialize a TimeWindow.

        Args:
            start (date|datetime): Start of the window.
            end (date|datetime): End of the window.

        Raises:
            ValueError: If start > end.
        """
        self.start = self._to_datetime(start)
        self.end = self._to_datetime(end)

        if self.start > self.end:
            raise ValueError("Start time must be before end time")

    @staticmethod
    def _to_datetime(val: date | datetime) -> datetime:
        """Convert date to datetime at midnight, leave datetime unchanged."""
        if isinstance(val, date) and not isinstance(val, datetime):
            return datetime.combine(val, datetime.min.time())
        elif isinstance(val, datetime):
            return val
        else:
            raise TypeError(f"Expected date or datetime, got {type(val)}")

    def contains(self, col_or_val: Column | date | datetime) -> Column | bool:
        """
        Check if a Spark column or a Python datetime/date is within the time window.

        Args:
            col_or_val (Column | date | datetime): Spark column or Python datetime/date.

        Returns:
            Column | bool: Boolean Spark column for Spark input, or Python boolean for datetime/date input.
        """
        if isinstance(col_or_val, Column):
            return (col_or_val >= F.lit(self.start)) & (col_or_val <= F.lit(self.end))
        elif isinstance(col_or_val, (date, datetime)):
            val = self._to_datetime(col_or_val)
            return self.start <= val <= self.end
        else:
            raise TypeError(f"Expected Column, date, or datetime, got {type(col_or_val)}")

    def duration(self) -> timedelta:
        """Compute the duration of the window."""
        return self.end - self.start

    def shift(self, delta: timedelta) -> "TimeWindow":
        """Shift the window by a timedelta."""
        return TimeWindow(self.start + delta, self.end + delta)

    def expand(self, before: timedelta = timedelta(0), after: timedelta = timedelta(0)) -> "TimeWindow":
        """Expand the window by given amounts before and after."""
        return TimeWindow(self.start - before, self.end + after)

    def __repr__(self):
        return f"TimeWindow({self.start!r}, {self.end!r})"