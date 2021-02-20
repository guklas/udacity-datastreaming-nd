import logging
import json
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.types import *
import pyspark.sql.functions as psf
import pprint
import pandas as pd


# TODO Create a schema for incoming resources
# This schema is derived from the police-department-calls-for-service.json
schema = StructType([
    StructField("crime_id", StringType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", StringType(), True),        # date/time is a string in the json file
    StructField("call_date", StringType(), True),
    StructField("offense_date", StringType(), True),
    StructField("call_time", StringType(), True),          # e.g. "23:57"
    StructField("call_date_time", StringType(), True),     # GK: need to convert to TimeStamp
    StructField("disposition", StringType(), True),        # a short code as in radio_code.json
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),              # "CA"
    StructField("agency_id", StringType(), True),          # e.g. "1"
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True),
])

radio_schema = StructType([                                # GK: I added a schema to read from file
        StructField("disposition_code", StringType(), True),
        StructField("description", StringType(), True)
    ])

def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    # GK: compared to classroom: there, maxOffsetsPerTrigger = 10
    # GK: Regarding maxRatePerPartition: I believe this parameter is not relevant for this version of the 
    #     structured streaming API, 2.3.4:
    """
     As per this URL (https://spark.apache.org/docs/latest/configuration.html), this parameter is only
     relevant for 'Kafka direct stream API', as the link to the relevant Kafka Integration guide points to
     Kafka integration with Spark Streaming, but not with Spark Structured Streaming:
     https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html.
     This parameter does not exist in the Spark 2.3.4 or 3.0.1 Structured Streaming + Kafka Integration Guide:
     https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
     Adding this option in below doesn't do any harm, but turned out to have no influence at all.
    """

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers","localhost:9092") \
        .option("subscribe", "sf.police.calls") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 2000) \
        .option("maxRatePerPartition", 1) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    print("***RSJ: schema of df after spark.readStream")
    df.printSchema() # a streaming dataset
    
    print("***RSJ: check number of partitions of the dataframe df") 
    print("***RSJ: type of df:", type(df)) 

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")
    
    print("***RSJ: schema of kafka_df after select and cast of value to string")
    kafka_df.printSchema() # GK: check schema of the df now
    
    # GK: each record is JSON-formatted string -> convert using the schema
    # GK: .from_json(): Parses a column containing a JSON string into a
    #     [[StructType]] with the specified schema
    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    # check the schema now
    print("***RSJ: schema of service_table")
    service_table.printSchema() # a streaming dataset
    
    # GK: service_table is now a dataframe with columns as per police-department... .json
    # TODO select original_crime_type_name and disposition
    # GK: vary and experiment with Watermark: 1 min, 10 min, ...
    # GK: call_data_time comes in as string, thus converting to time stamp
    distinct_table = service_table \
        .select(psf.col("original_crime_type_name"), 
                psf.col("disposition"), 
                psf.to_timestamp(psf.col("call_date_time")).alias("call_date_time")
               ) \
        .withWatermark("call_date_time", "60 minutes") # apply to a streaming dataset
    
    # check the schema now
    print("***RSJ: schema of distinct_table")
    distinct_table.printSchema() # a streaming dataset

    # count the number of original crime type
    agg_df = distinct_table \
        .dropna() \
        .groupby("original_crime_type_name") \
        .count() \
        .sort("count",ascending=False )

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
   
    print("***RSJ: schema of agg_df")
    agg_df.printSchema()
    
    print("***RSJ: type of agg_df:", type(agg_df))
    
    # GK: choice of trigger interval to see stuff on the console: 30 seconds
    
    #***********************************************************************
    # Aggregation query
    #***********************************************************************
    #''' turn query on or off for experimenting
    query = agg_df \
        .writeStream \
        .trigger(processingTime="30 seconds") \
        .format("console") \
        .outputMode("Complete") \
        .option("truncate", "false") \
        .queryName("Aggregation_Query") \
        .start()

    # TODO attach a ProgressReporter
    query.awaitTermination(3600) # proceed latest after 1 hour
    query.stop() # stop the query to avoid problems
    #'''

    # GK: below code only executes once above query is finished.
    
    # TODO get the right radio code json path
    # GK: https://spark.apache.org/docs/latest/sql-data-sources-json.html:
    #     For this type of JSON file, set: .option("multiline", "true")
    
    print("***RSJ: Reading radio code file from disk")
    radio_code_json_filepath = "radio_code.json" # file is in same folder as this script
    radio_code_df = spark \
        .read \
        .option("multiline", "true") \
        .json( path=radio_code_json_filepath, schema=radio_schema   ) # GK: add a schema as good practice
    
    # check the schema
    print("***RSJ: Schema of radio_code_df")
    radio_code_df.printSchema()

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition") # GK: for join
    print("***RSJ: radio_code_df: column renamed")

    # TODO join on disposition column
    # GK: This is a stream/static left join
    # GK: Error in Udacity code: join_query cannot work based on agg_df:
    # Reason: agg_df has no column disposition in it anymore
    
    # join_query = agg_df \
    #***********************************************************************
    # Join query
    #***********************************************************************
    #''' turn query on or off for experimenting
    join_query = distinct_table \
        .join(radio_code_df,
              distinct_table.disposition == radio_code_df.disposition,
              "left_outer") \
        .select("call_date_time", "original_crime_type_name", "description") \
        .writeStream \
        .trigger(processingTime="30 seconds") \
        .format("console") \
        .queryName("Join Query") \
        .option("truncate", "false") \
        .start()
    
    print("***SDS: join_query started")

    join_query.awaitTermination()
    #'''

if __name__ == "__main__":
    # set logger for console, defining suitable format
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)s  %(message)s', datefmt = logging.Formatter.default_time_format)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # TODO Create Spark in Standalone mode
    
    #********************************************************************************
    # creating spark config to feed into session builder to allow for experimentation 
    #********************************************************************************

    conf1 = SparkConf().setAll([ 
        ('spark.master', 'local[*]'),
        ('spark.executor.memory', '4g'),
        ('spark.driver.memory','4g'),
        ('spark.executor.cores', '2'),
        ('spark.cores.max', '2'),
        ('spark.sql.shuffle.partitions', '2'),
        ('spark.default.parallelism', '2'),
        ('spark.ui.port', 3000),
        ('spark.app.name', 'KafkaSparkStructuredStreaming')
    ]) 
    
    fs = "\n*** "+"-"*100+"\n" 
    print(fs + "*** MAI: creating SparkSession " + fs)
    
    spark = SparkSession \
        .builder \
        .config(conf=conf1) \
        .getOrCreate()
    
    logger.info("*** MAI Spark session created") 
    
    #********************************************************************************
    # print various configuration settings to check 
    #********************************************************************************
    
    config_now = spark.sparkContext._conf.getAll() # a list of tuples 
 
    print(fs + "*** MAI: showing Spark configuration in custom format " + fs)
    for ele1,ele2 in config_now:
        print("{:<30s}{:}".format(ele1,ele2))

    print(fs + "*** MAI: individual spark.conf.get(...) " + fs)  
    print("spark.sql.shuffle.partitions:\t", spark.conf.get( "spark.sql.shuffle.partitions"  ) ) # returns 200
    print("spark.default.parallelism:\t", spark.conf.get("spark.default.parallelism"))
    print("spark.default.parallelism:\t",  spark.sparkContext._conf.get("spark.default.parallelism" ) )

    print(fs + "*** MAI: spark.sql('SET')" + fs)    
    pd1 = spark.sql("SET -v").toPandas()
    print("*** MAI: type of pd1:", type(pd1)) # <class 'pandas.core.frame.DataFrame'>
    pd.set_option("display.max_rows", None, "display.max_columns", 10, "display.expand_frame_repr", True, "display.width", None)
    print(pd1)
    
    # set the log level 
    spark.sparkContext.setLogLevel("INFO") # default is INFO, reduced output with WARN
    
    logger.info("*** MAI: Spark started")

    run_spark_job(spark)

    spark.stop()
