import findspark

findspark.add_jars('/app/postgresql-42.1.4.jar')
findspark.init()

# https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

#import sys
import pytz
import datetime 

from datetime import timedelta
#from datetime import datetime
from time import sleep
import requests
import numpy as np
import pandas as pd
import json



#from pyspark.sql.functions import col

# UDF
#from pyspark.sql.types import StringType
#from pyspark.sql.window import Window



#************************ Definitions **************

BASE_URL = 'https://www.alphavantage.co/query'
API_KEY = 'TFHNYCWBD71JBSON'
#https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=aapl&interval=60min&apikey=TFHNYCWBD71JBSON&datatype=json
STOCK_FN = 'TIME_SERIES_DAILY' 
OUTPUTSIZE='full' # "compact" returns only the latest 100 data points; "full" returns the full-length time series of 20+
#STOCK_FN = 'TIME_SERIES_DAILY_ADJUSTED' #Premium endpoint not possible to use.
#TIME_SERIES_INTRADAY interval=5min

MANY_DAYS=365*5 #5 years aprox including 2020

# table's name and queries
SQL_DB = 'etl'  
SQL_TABLE = 'stocks'
SQL_CREATE = f"""
CREATE TABLE IF NOT EXISTS {SQL_TABLE} ( date TIMESTAMP, symbol TEXT, 
open REAL, close REAL,high REAL,low REAL,volume REAL,
avg_price REAL, ma3 REAL, ma5 REAL, ma15 REAL, ma30 REAL,
UNIQUE(date,symbol)
);
"""
#adj_close REAL,volume INTEGER,div_amount REAL,split_coef REAL ,
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

# »»»»»»»»»»»»»»»»» STOCKS ««««««««««««««««««««z

STOCKS={'amazon':'AMZN','apple':'AAPL','netflix':'NFLX','google':'GOOG','Accenture':'ACN',
       'alibaba':'BABA','Turtle Beach':'HEAR','Disney':'DIS',
       'LG Display':'LPL','microsoft':'MICS','sony':'SONY',
       'Cenovus Energy':'CVE','YPF':'YPF','SHELL':'SHEL','Petrobras':'PBR',
       'Coca-Cola':'KO','PespsiCo':'PEP','Unilever':'UL','Kimberly-Clark':'KMB',
       'Mondelez':'MDLZ'}

#STOCKS = {'amazon':'AMZN'}

# dir where parquete file will be created
stocks_dir = '/dataset/stocks_aux/'

#*****************************************************************

def _connect_spark():
    spark = (
        SparkSession.builder
        .appName("Extract_stocks")
        #.config("spark.driver.extraClassPath", '/app/postgresql-42.1.4.jar')
        .config("spark.driver.memory", "512m")
        .config("spark.driver.cores", "1")
        .config("spark.executor.memory", "512m")
        .config("spark.executor.cores", "1")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    return spark

def _read_last_date_updated(spark):
    df = spark.read.jdbc(url, SQL_TABLE, properties=properties)
    #df = spark.read.format('jdbc').options(driver = 'org.postgresql.Driver',url=url, dbtable=SQL_TABLE ).load()
    df.createOrReplaceTempView(SQL_TABLE)
    result_df = spark.sql(SQL_GET_DATE)
    #date = f"{context['execution_date']:%Y-%m-%d}"
 
    return result_df



def _get_stock_data(stock_symbol, list_date):
    stock_df=pd.DataFrame()
    for symbol in stock_symbol.values() :
        end_point = (
            f"{BASE_URL}?function={STOCK_FN}&symbol={symbol}"
            f"&outputsize={OUTPUTSIZE}&apikey={API_KEY}&datatype=json"
        )
        print(f"Getting data from {end_point}...")
        r = requests.get(end_point)
        sleep(20)  # To avoid api limits
        data = json.loads(r.content)
        #print(data)
        df = (
            pd.DataFrame(data['Time Series (Daily)'])
            .T.reset_index()
            .rename(columns={'index': 'date'})
        )
        #checking true dates to get data.    
        for date in list_date:
            df_aux=df
            #keep data from execution_date
            df_aux= df_aux[df_aux['date'] == date]
            #print(df_aux)
            if not df_aux.empty:
                for c in df_aux.columns:
                    if c != 'date':
                        #convert to floate
                        df_aux[c] = df_aux[c].astype(float)
                df_aux['avg_price'] = (df_aux['2. high'] + df_aux['3. low']) / 2
            else:
                df_aux = pd.DataFrame(
                    [[date,symbol,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan]],
                    #np.nan,np.nan,np.nan,np.nan,np.nan]],
                    columns=['date', 'symbol','open','close','high','low', 
                             #'adj_close','volume','div_amount','split_coef','avg_price']
                             'volume','avg_price']
                             )
            df_aux.rename(columns={'1. open': 'open','2. high': 'high','3. low': 'low','4. close': 'close','5. volume': 'volume'},
                    inplace=True )
            df_aux['symbol'] = symbol
            df_aux = df_aux[['date', 'symbol','open','close','high','low', 
                     'volume','avg_price']]
            #print(f"Stok {symbol} in date {date} was read and it is ready to be uploaded..")
            stock_df=stock_df.append(df_aux)
    return stock_df


def _check_data_frame(last_updated_date):
    #get local time in type of string
    localFormat = "%Y-%m-%d %H:%M:%S"
    #checking today date
    UTC_date=datetime.datetime.now(pytz.
                timezone('America/Argentina/Buenos_Aires')).strftime(localFormat)
    #convert to datetime
    end_date=datetime.datetime.strptime(UTC_date, localFormat)

    delta = timedelta(days=1)
    # store the dates in a list
    dates = []
    
    print(end_date-last_updated_date)  
    # check if difference is bigger than MANY_DAYS and we are not trying
    #to update same date.
    if  ((end_date-last_updated_date).days<=MANY_DAYS) & \
        ((end_date-last_updated_date).days>0):
        # increment in one the last update date in order to prevent
        # to upload in DB the this date (Duplicate error in DB)    
        last_updated_date += delta   
        #Create list 
        while last_updated_date<=end_date:
            # add current date to list by converting  it to iso format
            dates.append(f"{last_updated_date:%Y-%m-%d}")
            # increment start date by timedelta
            last_updated_date += delta
    return dates


if __name__ == "__main__":
    
    spark = _connect_spark()
    print("Started Spark Session...")
    df=_read_last_date_updated(spark)
    print("Get last update date from database")
    #check if posgres is empty or the last updated date.
    #https://sparkbyexamples.com/pyspark/pyspark-isnull/#:~:text=pyspark.,returns%20a%20boolean%20value%20True.
    
    if df.filter(F.col("date").isNotNull()).count()>0:
        last_updated_date = df.first()['date']
    else:
        last_updated_date=datetime.datetime(2018, 11, 1)
    print(last_updated_date)
    list_date=_check_data_frame(last_updated_date)

    #check if date list is not empty. if it is empty, there is nothing to be updated.
    if list_date:
        print("Uploading data to Database...") 
        df=_get_stock_data(STOCKS, list_date)
        df.to_parquet(stocks_dir+"aux_file.parquet")
     
        with open(stocks_dir+"status.txt", "w") as f:
            f.write("xxxxxxxxxxxxxxxx")
    else:
        print(f"Database is already updated. Last update {last_updated_date}")

        with open(stocks_dir+"status.txt", "w") as f:
            f.write("Database is already updated.")
    print("All done")
    print("Spark was stopped")
    spark.stop()
