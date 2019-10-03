#pip install boto3
#pip install pyspark , conda install pyspark
#"~/.aws/credentials" file should have details for keys and password
import math
import psycopg2
from configparser import ConfigParser
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import IntegerType
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, concat
import os
import string
from pyspark.sql.functions import *
from pyspark.sql.functions import when, lit, col
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import explode
conn = None

def read_from_s3(file_name):
    config = ConfigParser()
    config.read(os.path.expanduser("~/.aws/credentials"))                  
    aws_profile = 'default'                                
    access_id = config.get(aws_profile, "aws_access_key_id") 
    access_key = config.get(aws_profile, "aws_secret_access_key")                  

    spark = SparkSession.builder.appName("app").getOrCreate();
    sc = spark.sparkContext
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_id)
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", access_key)

    csvDf = spark.read.option("header", "true").option("inferschema", "true").csv(file_name)
    selected_df = csvDf.select("longitude","latitude","location_id", "borough")
    selected_df.show()
    return selected_df
              
def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)
 
    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
 
    return db


def truncate(number, digits) -> float:
    stepper = 10.0 ** digits
    return math.trunc(stepper * number) / stepper

def getdb_connection():
    try:
        global conn
        if conn is None:
            print('connection is null creating new ')
            # read database configuration
            params = config()
            # connect to the PostgreSQL database
            conn = psycopg2.connect(**params)
            return conn;
        else:
            #print('connection is already created')
            return conn;
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

sql = """INSERT INTO dataengproj.locations_meta_data(location_id,detail,geometry)
        VALUES(%s,%s,ST_SetSrid(ST_MakePoint(%s,%s),4326));"""

def insert_record(location_id,longitude,latitude,borough):
    try:
        conn = getdb_connection1()
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (location_id,borough,longitude,latitude))
        conn.commit()
        # close communication with the database
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        if conn is not None:
            conn.close()

def process_record_taxi(df):
    # longitude,latitude,location_id,borough
    for row in df.rdd.collect():
        latitude = truncate(row.latitude,4)
        longitude = truncate(row.longitude,4)
        insert_record(row.location_id,longitude,latitude,row.borough)
    
def process_record_crime(df):
    # longitude,latitude,location_id,borough
    number=500
    for row in df.rdd.collect():
        number+=1
        borough='location name'
        print(number)
        insert_record(number,row.longitude,row.latitude,borough)

    global conn    
    conn.close()    

            
def write_df_to_pgsql(df, table_name):
    db_properties = config() 
    user_name = db_properties['username']
    password = db_properties['password']
    db_url =  db_properties['url']    

    df.write \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", table_name) \
        .option("user", user_name) \
        .option("password", password) \
        .mode("append").save()
              
if __name__ == '__main__':
    filename_taxi="s3a://my-nyc-taxi/taxi_station_geo_mapping_1.csv"
    filename_crime="s3a://nyc-crime/2019_crime_meta_data.csv"

    df_taxi = read_from_s3(filename_taxi);
    #df_taxi = df_taxi.limit(3)
    process_record_taxi(df_taxi)
    
    df_crime = read_from_s3(filename_crime);
   # df_crime = df_crime.limit(3)
    process_record_crime(df_crime)
        
   
        

