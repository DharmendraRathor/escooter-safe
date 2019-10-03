#import boto3
#import psycopg2
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


def find_score(distance,psg_count,amount):
  score = 0
  for i in distance:
    if i and float(i) < 2:
        score +=1

  for i in psg_count:
    if i and int(i) > 1:
        score+=1

  for i in amount:
    if i and float(i) > 5:
        score+=1

  return score

# def read_from_s3_usingboto3_client():
#     s3 = boto3.client('s3')
#     response = s3.list_buckets()
#     print('Existing buckets:')

#     for bucket in response['Buckets']:
#         print(f'  {bucket["Name"]}')
#     #obj = boto3.resource('s3').Bucket('my-nyc-taxi')

#     data = s3.get_object(Bucket='my-nyc-taxi', Key='yellow_tripdata_2019_sample1.csv')
#     res = data['Body'].read()
#     res = str(res,'utf-8')


#     schema = StructType([
#         StructField("VendorID", IntegerType(), True),
#         StructField("passenger_count", IntegerType(), True),
#         StructField("trip_distance", IntegerType(), True),
#         StructField("PULocationID", IntegerType(), True),
#         StructField("DOLocationID", IntegerType(), True),
#         StructField("total_amount", IntegerType(), True)
#     ])

#     #0,3,4,7,8,16
#     #VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,congestion_surcharge
#     res = res.splitlines()[1:]
#     lines = []
#     for line in res:
#         line = line.split(',')
#         line = [int(line[0]),int(line[3]),int(float(line[4])),int(line[7]),int(line[8]),int(float(line[16]))]
#         lines.append(line)
#         print(line)

#     spark = SparkSession.builder.appName("application").getOrCreate();
#     sc = spark.sparkContext

#     print(lines)
#     df = spark.createDataFrame(spark.sparkContext.parallelize(lines),schema)
#     return df

def read_from_s3(file_name_in):
    config = ConfigParser()
    config.read(os.path.expanduser("~/.aws/credentials"))
    aws_profile = 'default'
    access_id = config.get(aws_profile, "aws_access_key_id")
    access_key = config.get(aws_profile, "aws_secret_access_key")

    spark = SparkSession.builder.appName("app").getOrCreate();
    sc = spark.sparkContext
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_id)
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", access_key)

    csvDf = spark.read.option("header", "true").option("inferschema", "true").csv(file_name_in)
    selected_df = csvDf.select("VendorID","tpep_pickup_datetime","passenger_count", "trip_distance","PULocationID","DOLocationID","total_amount")
    selected_df.show()
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
    db_url =  db_properties['url']
    df.write \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", table_name) \
        .option("user", user_name) \
        .option("password", password) \
        .mode("append").save()


def date_conversion(indf):
    outdf = indf.withColumn('ARREST_DATE', unix_timestamp('ARREST_DATE', 'MM/dd/yyy')
    .cast("timestamp")).alias('ARREST_DATE')
    outdf.show()
    return outdf


# def writeToDb(df, table_name):
#    db_properties = config()
#    #db_url = db_properties['url']
#    db_url = 'jdbc:postgresql://54.149.81.250:5432/dataengg?sslmode=require&sslfactory=org.postgresql.ssl.NonValidatingFactory'
#    print(db_properties)
#    print(db_url)
#    df.write.jdbc(url=db_url,table=table_name,mode='append',properties=db_properties)
#    print('data insertion complete')

def processEachFile(file):
    df = read_from_s3(file);
    #df.show()
   # df = df.withColumn('tpep_pickup_datetime', concat(df.tpep_pickup_datetime.substr(0, 13), lit(':00:00')))
    df = df.withColumn('tpep_pickup_datetime',(round(unix_timestamp(col("tpep_pickup_datetime")) / 3600) * 3600).cast("timestamp"))
    df.printSchema();

    df = df.withColumnRenamed('tpep_pickup_datetime','event_time').withColumnRenamed('passenger_count','psg_count').withColumnRenamed('trip_distance','trip_distance').withColumnRenamed('PULocationID','location_id').withColumnRenamed('total_amount','amount')
    #df.show()
    df = df.dropna()
    find_score_udf = F.udf(find_score, T.IntegerType())
                #    df=df.groupBy('pickup_hour','location').agg(find_score_udf(F.collect_list('trip_distance'),F.collect_list('psg_count'),F.collect_list('amount')).alias('score'))
    df=df.groupBy('event_time','location_id').agg(F.avg('trip_distance').cast(T.IntegerType()).alias('distance_avg') ,F.avg('psg_count').cast(T.IntegerType()).alias('psg_count_avg'),F.avg('amount').cast(T.IntegerType()).alias('amount_avg'),F.count('location_id').alias('count'),find_score_udf(F.collect_list('trip_distance'),F.collect_list('psg_count'),F.collect_list('amount')).alias('score'))
    df.show()
    write_df_to_pgsql(df,'dataengproj.taxi_travel_score1')
    print(' file processed ',file)


if __name__ == '__main__':
    #for file in file_names:
    file='s3a://mynytc/trip*data/yellow_tripdata*csv'
    processEachFile(file)

