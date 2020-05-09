from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
import pyspark.sql.functions as sf

spark = SparkSession.builder.appName("assignment1").getOrCreate()
path="path to the csv file or table"
#read the data here assuming csv data
df = spark.read.csv("path")
df = df.withColumn("id", df["id"].cast(IntegerType())).withColumn("people_count", df["people_count"].cast(IntegerType()))
w = Window.orderBy("id")
df = df.withColumn("people_lag_1", sf.lag(df["people_count"]).over(w)).withColumn("people_lag_2", sf.lag(df["people_count"], 2).over(w))

df_100 = df.filter(df["people_count"]+df["people_lag_1"] + df["people_lag_2"] >= 100)
spark.conf.set("spark.sql.crossJoin.enabled" "true")
df_100 = df_100.withColumnRenamed("event_name","evenet_name_100").withColumnRenamed("id","id_100").withColumnRenamed("people_count","people_count_100")
df_output = df.join(df_100).filter((df["id"]+2 == df_100["id_100"]) | (df["id"]+1 == df_100["id_100"]) | (df["id"] == df_100["id_100"]))
df_output.select("id","event_name","people_count").show()

#to run this code """ spark-submit --driver-memory 4G --executor-memory 10G --num-executors 50 --executor-cores 5 pyspark_sql.py"""