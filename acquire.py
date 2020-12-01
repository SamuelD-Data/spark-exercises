# establishing environment 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

def get_311_data():
    """
    No arguments needed. Function returns 311 data separated into respective DFs.
    """
    # reading in data and saving to separate DFs
    source = spark.read.csv("source.csv", sep=",", header=True, inferSchema=True)
    case = spark.read.csv("case.csv", sep=",", header=True, inferSchema=True)
    dept = spark.read.csv("dept.csv", sep=",", header=True, inferSchema=True)

    # returning DFs
    return source, case, dept


