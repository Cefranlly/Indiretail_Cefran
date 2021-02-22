from unittest import TestCase
from pyspark.sql import SparkSession
from lib.utils.utils import load_parquet_to_df


class UtilsTestCase(TestCase):
    spark = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
            .master("local[3]") \
            .appName("HelloSparkTest") \
            .getOrCreate()

    def test_datafile_loading(self):
        sample_df = load_parquet_to_df(self.spark, "data/sample.parquet")
        result_count = sample_df.count()
        self.assertEqual(result_count, 100, "Record count should be 100")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
