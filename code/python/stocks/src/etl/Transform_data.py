import sys

# https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# UDF
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

#************************ Definitions **************


# dir where parquete file will be created
stocks_dir = '/dataset/stocks_aux/'

#*****************************************************************


def _connect_spark():
    spark = (
        SparkSession.builder
        .appName("Transform_stocks")
        .config("spark.driver.memory", "512m")
        .config("spark.driver.cores", "1")
        .config("spark.executor.memory", "512m")
        .config("spark.executor.cores", "1")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    return spark


#Reading parquet file
def _read_input_file(spark,stocks_folder):

    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .parquet(stocks_folder+"aux_file.parquet")
   
    )
    return df

#Writing parquet file
def _write_output_file(df,stocks_folder):

    df.write.mode('overwrite') \
    .parquet(stocks_folder+"to_pogrest_file.parquet")


if __name__ == "__main__":

# Initialization


    spark = _connect_spark()

    print("Started Spark Session...")

    df_stocks = _read_input_file(spark,stocks_dir)
    print("Sample of df_stocks data:")
    df_stocks.show(3)

    print("Calculate windows")

    # Calculate Moving Average
    # https://stackoverflow.com/questions/45806194/pyspark-rolling-average-using-timeseries-data

    #UDF (User Defined Function)
    window3 = (
        Window.partitionBy(F.col('symbol')).orderBy(F.col("date")).rowsBetween(-3, 0)
    )

    window5 = (
        Window.partitionBy(F.col('symbol')).orderBy(F.col("date")).rowsBetween(-5, 0)
    )
    window15 = (
        Window.partitionBy(F.col('symbol')).orderBy(F.col("date")).rowsBetween(-15, 0)
    )
    window30 = (
        Window.partitionBy(F.col('symbol')).orderBy(F.col("date")).rowsBetween(-30, 0)
    )

    # // Calculate the moving average
    stocks_moving_avg_df = (
        df_stocks.withColumn("ma3", F.avg("close").over(window3))
        .withColumn("ma5", F.avg("close").over(window5))
        .withColumn("ma15", F.avg("close").over(window15))
        .withColumn("ma30", F.avg("close").over(window30))
    )

    print("Sample of stocks_moving_avg_df data:")
    stocks_moving_avg_df.show()
    print("Saving file")
    _write_output_file(stocks_moving_avg_df,stocks_dir)
  
    print("All done")
    print("Spark was stopped")
    spark.stop()
