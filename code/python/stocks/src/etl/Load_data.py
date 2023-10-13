import findspark

findspark.add_jars('/app/postgresql-42.1.4.jar')
findspark.init()

import sys

# https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import datetime

#********************table's name and queries**********************
SQL_DB = 'etl'  
SQL_TABLE = 'stocks'

SQL_GET_DATE = f"""
SELECT date FROM {SQL_TABLE} 
ORDER BY date DESC LIMIT 1 
"""

url = "jdbc:postgresql://postgres/etl"

properties = {
    "user": "marcos",
    "password": "m0rc05",
    "driver": "org.postgresql.Driver"
}
#*****************************************************************
#************************ Definitions **************


# dir where parquete file will be created
stocks_dir = '/dataset/stocks_aux/'


#*****************************************************************


def _connect_spark():
    spark = (
        SparkSession.builder
        .appName("Load_stocks")
        .config("spark.driver.memory", "512m")
        .config("spark.driver.cores", "1")
        .config("spark.executor.memory", "512m")
        .config("spark.executor.cores", "1")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    return spark

#Reading postgres file
def _read_last_date_updated(spark):
    df = spark.read.jdbc(url, SQL_TABLE, properties=properties)
    #df = spark.read.format('jdbc').options(driver = 'org.postgresql.Driver',url=url, dbtable=SQL_TABLE ).load()
 
    df.createOrReplaceTempView(SQL_TABLE)
    result_df = spark.sql(SQL_GET_DATE)
    #date = f"{context['execution_date']:%Y-%m-%d}"
 
    return result_df

#Writing postgres file
def _write_postgres(df):

    # Write to Postgres  remove dbtable etl.stocks to stocks 
    df.drop("__index_level_0__") \
    .write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres/etl") \
    .option("dbtable", "stocks") \
    .option("user", "marcos") \
    .option("password", "m0rc05") \
    .option("driver", "org.postgresql.Driver") \
    .mode('append') \
    .save()
    
#Reading parquet file
def _read_input_file(spark,stocks_folder):

    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .parquet(stocks_folder+"to_pogrest_file.parquet")
   
    )
    return df


if __name__ == "__main__":

# Initialization


    spark = _connect_spark()

    print("Started Spark Session...")
    #read parquet
    df_stocks = _read_input_file(spark,stocks_dir)
    # Using Cast to convert TimestampType to DateType
    #Timestamp String to DateType
    df_stocks=df_stocks.withColumn("date",F.to_timestamp("date"))
    print("Sample of df_stocks data:")
    df_stocks.show(3)
    print("Reading parquet file...last updated date") 
    last_updated_date = df_stocks.sort(["date"], ascending=[False]).first()['date']

    print("Reading Database...last updated date") 
    df_database=_read_last_date_updated(spark)

    if df_database.filter(F.col("date").isNotNull()).count()>0:
        db_date = df_database.first()['date']
    else:
        db_date=datetime.datetime(2018, 11, 1)
    print("Parquet " +str(last_updated_date),"Data Base "+str(db_date))

    # if data base date is less than parquet file, data base must be updated. 
    if db_date <last_updated_date:
        print("Saving postgres")
        _write_postgres(df_stocks)
    else:
        print(f"Database is already updated. Last update {last_updated_date}")

    print("All done")
    print("Spark was stopped")
    spark.stop()

 