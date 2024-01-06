from contextlib import contextmanager

from pyspark.sql import SparkSession


@contextmanager
def spark_session(appname="dps-process") -> SparkSession:
    spark = None
    try:
        spark = SparkSession.builder \
            .appName(appname) \
            .getOrCreate()
        yield spark
    finally:
        if spark is not None:
            spark.stop()