import pyspark 
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Read DataFrame Example") \
    .getOrCreate()
spark
df = spark.read.csv("recording_request.csv", header=True, inferSchema=True)
city_zone_df=df.groupBy("City","Zone").count()
speciality_zone_df=df.groupBy("Speciality","Zone").count()
joined_df = city_zone_df.join(speciality_zone_df, on="Zone", how="inner")
joined_df.show()
