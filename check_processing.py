
## This code is just to check if our connection with EMR is working or now 

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("souce process").getOrCreate()

source_df = spark.read.csv("s3://pysparkapi/banktxn/csv/banktxn.csv", header=True)

# to write into parquet format. 
 
source_df.write.parquet("s3://pysparkapi/banktxn/parquet") 



