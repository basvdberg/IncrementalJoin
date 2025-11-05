import unittest
from datetime import datetime, date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from time_utils import TimeWindow


class TestTimeWindow(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("TimeWindowTest") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_init_datetime_and_date(self):
        dt_start = datetime(2025, 11, 4, 10, 0)
        dt_end = datetime(2025, 11, 4, 12, 0)
        date_start = date(2025, 11, 4)
        date_end = date(2025, 11, 5)

        # datetime init
        tw_dt = TimeWindow(dt_start, dt_end)
        self.assertEqual(tw_dt.start, dt_start)
        self.assertEqual(tw_dt.end, dt_end)

        # date init
        tw_date = TimeWindow(date_start, date_end)
        self.assertEqual(tw_date.start, datetime.combine(date_start, datetime.min.time()))
        self.assertEqual(tw_date.end, datetime.combine(date_end, datetime.min.time()))

        # start > end should raise ValueError
        with self.assertRaises(ValueError):
            TimeWindow(dt_end, dt_start)

    def test_contains_python(self):
        tw = TimeWindow(datetime(2025, 11, 3,10, 20), datetime(2025, 11, 4, 12,30))
        inside = datetime(2025, 11, 4, 11,0)
        outside = datetime(2025, 11, 4, 13, 12,31)
        self.assertTrue(tw.contains(inside))
        self.assertFalse(tw.contains(outside))

        # Also test with date input
        inside_date = date(2025, 11, 4)
        self.assertTrue(tw.contains(inside_date))

    def test_contains_spark_column(self):
        tw = TimeWindow(datetime(2025, 11, 4, 10), datetime(2025, 11, 4, 12))
        data = [
            (datetime(2025, 11, 4, 9),),
            (datetime(2025, 11, 4, 11),),
            (datetime(2025, 11, 4, 13),)
        ]
        df = self.spark.createDataFrame(data, ["ts"])
        filtered = df.filter(tw.contains(col("ts"))).collect()
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["ts"], datetime(2025, 11, 4, 11))

    def test_duration_shift_expand(self):
        start = datetime(2025, 11, 4, 10)
        end = datetime(2025, 11, 4, 12)
        tw = TimeWindow(start, end)

        # duration
        self.assertEqual(tw.duration(), timedelta(hours=2))

        # shift
        tw_shifted = tw.shift(timedelta(hours=1))
        self.assertEqual(tw_shifted.start, start + timedelta(hours=1))
        self.assertEqual(tw_shifted.end, end + timedelta(hours=1))

        # expand
        tw_expanded = tw.expand(before=timedelta(minutes=30), after=timedelta(minutes=30))
        self.assertEqual(tw_expanded.start, start - timedelta(minutes=30))
        self.assertEqual(tw_expanded.end, end + timedelta(minutes=30))

if __name__ == "__main__":
    unittest.main()
