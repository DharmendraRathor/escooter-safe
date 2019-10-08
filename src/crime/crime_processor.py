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


csvfile_name="s3a://nyc-crime/NYPD_Arrest_Data_2019.csv"
csvfile_history_name="s3a://nyc-crime/NYPD_Arrests_Data__Historic.csv"

def find_score(distance,psg_count,amount):
  score = 0
  for i in distance:
    if float(i) < 2:
        score +=1

  for i in psg_count:
    if i > 1:
        score+=1

  for i in amount:
    if float(i) > 5:
        score+=1

  return score

def read_from_s3():
    config = ConfigParser()
    config.read(os.path.expanduser("~/.aws/credentials"))
    aws_profile = 'default'
    access_id = config.get(aws_profile, "aws_access_key_id")
    access_key = config.get(aws_profile, "aws_secret_access_key")

#    spark = SparkSession.builder.appName("app").getOrCreate();
    sc = spark.sparkContext
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_id)
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", access_key)
    csvDf = spark.read.option("header", "true").option("inferschema", "true").csv(csvfile_history_name)
    selected_df = csvDf.select("ARREST_DATE","PD_DESC","OFNS_DESC","KY_CD","PD_CD","Latitude","Longitude")
    #selected_df.show()
    return selected_df

def config(filename='dbproperties.ini', section='postgresql'):
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

def write_df_to_pgsql(df, table_name):
    db_properties = config()
    user_name = db_properties['user']
    password = db_properties['password']
    db_url =  db_properties['host']
    df.write \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", table_name) \
        .option("user", user_name) \
        .option("password", password) \
        .mode("append").save()


def read_df_from_pgsql():
    db_properties = config()
    db_tablename='dataengproj.locations_meta_data'
    user_name = db_properties['user']
    password = db_properties['password']
    db_url =  db_properties['host']
    df = spark.read \
        .format("jdbc") \
        .option("url", db_url) \
        .option("query", "SELECT location_id,ST_X(geometry), ST_Y(geometry)FROM dataengproj.locations_meta_data") \
        .option("user", user_name) \
        .option("password", password) \
        .load()
    return df

# code to fetch if any locaiton is not mapped to meta data table
def  collect_missing_location_meta_data(crime_df,location_metadf):
    #crime_df.show()
    new_location_meta_df = crime_df.join(location_metadf, on=['Latitude','Longitude'],how='left').filter(F.col('location_id').isNull())
    new_location_meta_df = new_location_meta_df.dropDuplicates(['Latitude','Longitude'])
    new_location_meta_df.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("meta_data_historic.csv")

def date_conversion(indf):
    #df = spark.createDataFrame([("11/25/2019",), ("11/24/1991",), ("11/30/1991",)], ['date_str'])
    #outdf = indf.withColumn('ARREST_DATE', from_unixtime(unix_timestamp('ARREST_DATE', 'MM/dd/yyy')).alias('ARREST_DATE'))
    outdf = indf.withColumn('ARREST_DATE', unix_timestamp('ARREST_DATE', 'MM/dd/yyy')
    .cast("timestamp")).alias('ARREST_DATE')
    outdf.show()
    return outdf

def  merge_and_save_crime_score(crime_df,location_metadf):
    crime_df= crime_df.withColumn("Latitude",(crime_df.Latitude*10000).cast(T.IntegerType())/10000)
    crime_df= crime_df.withColumn("Longitude",(crime_df.Longitude*10000).cast(T.IntegerType())/10000)
    crime_with_location_df = crime_df.join(location_metadf, on=['Latitude','Longitude'],how='left').filter(F.col('location_id').isNotNull())
    #crime_with_location_df.withColumn('ARREST_DATE', concat(df.ARREST_DATE.substr(0, 13), lit(':00:00')))
    crime_with_location_df.show()
    #print('crime data count before join ', crime_with_location_df.count())
    keycd_desc_df = crime_with_location_df.select('KY_CD','OFNS_DESC').dropDuplicates(['KY_CD']).filter(F.col('KY_CD').isNotNull())
    print(' printing data frames ')
    keycd_desc_df.show()
    crime_with_location_df = crime_with_location_df.groupBy('ARREST_DATE','location_id','KY_CD').count().alias("count")
    crime_with_location_df.show()
    crime_with_location_df = crime_with_location_df.join(keycd_desc_df,on=['KY_CD'],how='left')

    #print('crime data count after join ', crime_with_location_df.count())
    crime_with_location_df =crime_with_location_df.withColumnRenamed('ARREST_DATE','event_time').withColumnRenamed('KY_CD','crime_ky_cd').withColumnRenamed('OFNS_DESC','crime_description')

    crime_with_location_df.show()
    # print and save to db.
    write_df_to_pgsql(crime_with_location_df,'dataengproj.crime_arrest_score')


if __name__ == '__main__':
     spark = SparkSession.builder.appName("app").getOrCreate();
     df_crime_data = read_from_s3();
     df_crime_data = date_conversion(df_crime_data)
     df_crime_data= df_crime_data.withColumn("Latitude",(df_crime_data.Latitude*10000).cast(T.IntegerType())/10000)
     df_crime_data= df_crime_data.withColumn("Longitude",(df_crime_data.Longitude*10000).cast(T.IntegerType())/10000)
     df_crime_data.show()

     df_meta_location = read_df_from_pgsql().cache()
     df_meta_location = df_meta_location.withColumnRenamed('st_x','Longitude').withColumnRenamed('st_y','Latitude')

     df_meta_location= df_meta_location.withColumn("Latitude",(df_meta_location.Latitude*10000).cast(T.IntegerType())/10000)
     df_meta_location= df_meta_location.withColumn("Longitude",(df_meta_location.Longitude*10000).cast(T.IntegerType())/10000)
     df_meta_location.show()
     #df_meta_location.show()
     merge_and_save_crime_score(df_crime_data,df_meta_location)
