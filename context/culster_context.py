from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkConf


class ClusterContext:
    def __init__(self):
        self.get_spark_context()

    def get_spark_context(self):
        conf = (SparkConf()
                .set("spark.master", "yarn"))
        self.spark = SparkSession.builder.config(conf=conf).appName("bcgpartnertest").\
            enableHiveSupport.getOrCreate()
        self.sc = self.spark.sparkContext
        self.sqlc = SQLContext(self.sc)


