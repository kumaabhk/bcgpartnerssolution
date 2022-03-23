from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkConf


class Context:
    def __init__(self):
        self.get_spark_context()

    def get_spark_context(self):
        conf = (SparkConf()
                .set("spark.master", "local[*]")
                .set("spark.cpre.max", 4)
                .set("spark.driver.memory", '10g')
                .set("spark.sql.shuffle.partition", 4)
                .set("spark.executor.instances", 4)
                .set("spark.executor.pyspark.memory", "10g"))
        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()
