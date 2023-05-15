from unittest import TestCase
from pyspark.sql import SparkSession
from utils import *


class UtilsTestCases(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
            .master('local[3]') \
            .appName('HelloSparkTest') \
            .getOrCreate()

    def datafile_loading(self):
        test_df = read_dataframe(self.spark, 'datafiles/dataset.csv')
        result_count = test_df.count()
        self.assertEquals(result_count, 307716, f'Records Should Be {result_count}')

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
