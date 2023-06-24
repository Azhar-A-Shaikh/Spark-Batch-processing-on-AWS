# spark-submit --packages mysql:mysql-connector-java:8.0.22 file.py
# The above command is a spark submit command which can also be used for daily batch processing. 

import boto3
from datetime import datetime
from pyspark.sql.types import (
    StringType,
    DateType,
    FloatType,
)
from pyspark.sql.functions import (
    col,
    when,
    trim,
    to_timestamp,
)
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Currency Batch Job").getOrCreate()

df_bank_statement = spark.read.parquet("s3://pysparkapi/banktxn/parquet/") # Replace the S3 bucket name with your own.
df_bank_statement = (
    df_bank_statement.withColumn(
        "value_date_formatted",
        to_timestamp(df_bank_statement.value_date, "dd-MMM-yy")
        .cast(DateType())
        .cast(StringType()),
    )
    .drop("value_date")
    .withColumnRenamed("value_date_formatted", "value_date")
)
df_bank_statement = df_bank_statement.withColumn(
    "withdrawal_currency",
    when(trim(col("withdrawal_currency")) == "", None).otherwise(
        col("withdrawal_currency")
    ),
)

df_ticker_price_sgd = spark.read.parquet("s3://pysparkapi/response/*/") # Here replace you can replace the S3 location with your bucket name. 
df_bank_statement_null = df_bank_statement.filter("withdrawal_currency is null")
df_bank_statement_not_null = df_bank_statement.filter("withdrawal_currency is not null")

df_bank_statement = (
    df_bank_statement.alias("t1")
    .join(
        df_ticker_price_sgd.alias("t2"),
        (
            (col("t1.value_date") == col("t2.run_date"))
            & (trim(col("t1.withdrawal_currency")) == trim(col("t2.target_currency")))
        ),
        "left",
    )
    .select("t1.*", "t2.rates_base_sgd")
    .withColumnRenamed("rates_base_sgd", "withdrawal_sgd")
)

df_bank_statement = (
    df_bank_statement.alias("t1")
    .join(
        df_ticker_price_sgd.alias("t2"),
        (
            (col("t1.value_date") == col("t2.run_date"))
            & (trim(col("t1.deposit_currency")) == trim(col("t2.target_currency")))
        ),
        "left",
    )
    .select("t1.*", "t2.rates_base_sgd")
    .withColumnRenamed("rates_base_sgd", "deposit_sgd")
)


df_bank_statement = df_bank_statement.withColumn(
    "deposit_sgd_amt",
    col("deposit_sgd").cast(FloatType()) * col("deposit_amt").cast(FloatType()),
).withColumn(
    "withdrawal_sgd_amt",
    col("withdrawal_sgd").cast(FloatType()) * col("withdrawal_amt").cast(FloatType()),
)

df_bank_statement.write.parquet(s3://pysparkapi/banktxn/results/) # Creating a folder for the results 
