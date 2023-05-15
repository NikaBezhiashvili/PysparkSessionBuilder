from pyspark import *
from pyspark.sql import SparkSession
import os
import sys
from logger import Log4J


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['LOG4J_CONFIGURATION'] = 'file:C:/Users/Administrator/Desktop/Data/HelloSpark/log4j2.properties'


conf = SparkConf() \
        .setMaster('local[3]') \
        .setAppName('HelloRDD')

# sc = SparkContext(conf=conf)

spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()

logger = Log4J(spark)

sc = spark.sparkContext

linesRDD = sc.textFile('datafiles/dataset.csv')

partitionedRDD = linesRDD.repartition(2)

colsRDD = partitionedRDD.map(lambda line: line.replace('"', '').split(","))

