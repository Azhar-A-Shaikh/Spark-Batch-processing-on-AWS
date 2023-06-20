
## This code is just to check if our connection with EMR is working or now 

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("souce process").getOrCreate()

source_df = spark.read.csv("Replace your S3 location here", header=True)

# to write into parquet format. 
 
source_df.write.parquet("Replace your S3 location here")



