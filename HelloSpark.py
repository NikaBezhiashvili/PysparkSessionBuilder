from pyspark.sql import *
import os
import sys
from logger import Log4J
from utils import *

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['LOG4J_CONFIGURATION'] = 'file:C:/Users/Administrator/Desktop/Data/HelloSpark/log4j2.properties'

spark = SparkSession.builder \
    .appName("Hello Spark") \
    .master('local[3]') \
    .config('spark.sql.shuffle.partitions', '2') \
    .getOrCreate()

logger = Log4J(spark)

logger.info('Starting HelloSpark')

rawData = read_dataframe(spark, "datafiles/dataset.csv")

logger.info(rawData.columns)

partitioned_data = rawData.repartition(2)

filtered_data = filter_data(partitioned_data)

logger.info(rawData.count())
logger.info(filtered_data.collect())

rawData.createOrReplaceTempView('krn_clients')
KrnClients = spark.sql("select City from krn_clients")

KrnClients.show()

logger.info('Finished HelloSpark')

spark.stop()
