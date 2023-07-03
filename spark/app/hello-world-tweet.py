import sys
from pyspark.sql import SparkSession, functions
from pyspark.sql.types import StringType
from modules import moduleExample

# initialise sparkContext
spark = SparkSession.builder \
    .master('spark://spark:7077') \
    .appName('TwitterAggregationsAirflow') \
    .config('spark.executor.memory', '1gb') \
    .config("spark.cores.max", "1") \
    .getOrCreate()

sc = spark.sparkContext

# using SQLContext to read parquet file
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

# to read parquet file
df_csv = sqlContext.read.parquet('/opt/spark/resources/data/parc/*')

df_csv_sample = moduleExample.pysparkFunctions.sample_df(df_csv, 0.1)

print("Number of rows after sampling: {}".format(df_csv_sample.count())) 
print("######################################")

print("######################################")
print("ASSIGNING UUID")
print("######################################")

# Applying the python function. We don't need to create UDF in spark since spark version 3.1
df_csv_sample = df_csv_sample.withColumn("uuid", moduleExample.pythonFunctions.generate_uuid())

print("######################################")
print("PRINTING 10 ROWS OF SAMPLE DF")
print("######################################")

df_csv_sample.show(10)