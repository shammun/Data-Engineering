# import necessary libraries
import pandas as pd
import pyspark
import configparser
from datetime import datetime
import os
import glob
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, weekofyear, date_format, to_date
from pyspark.sql.functions import lit, expr
from pyspark.sql.functions import monotonically_increasing_id

config = configparser.ConfigParser()
config.read('config.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
AWS_ACCESS_KEY_ID = config.get('AWS', 'AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

# S3 bucket where the data will bne uploaded
DEST_S3_BUCKET = config['S3']['DEST_S3_BUCKET']

# Create Spark sessions
# Read using spark
spark = SparkSession.builder\
        .config("spark.jars.repositories", "https://repos.spark-packages.org/")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .enableHiveSupport().getOrCreate()
# Then setup the sparkContext object
sc = spark.sparkContext
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)

# path to the address in S3 where the fact and dimension tables will be written as parquet files
write_to_path = {"immigration": "final_project/immigration.parquet",
                "state": "final_project/state.parquet",
                "city": "final_project/city.parquet",
                "temperature": "final_project/temperature.parquet",
                "demographic": "final_project/demographic.parquet",
                "visa": "final_project/visa.parquet",
                "countries": "final_project/countries.parquet",
                "mode": "final_project/mode.parquet",
                "airport": "final_project/airport.parquet"}


def process_immigration_data(output):
    """
    Process immigration data and then write it to S3 as parqueet files
    
    Arguments:
        output: Target S3 endpoint
        
    Returns:
        None
    """
    logging.info("Immigration data is prepared now for uploading to S3")
    immigration_spark = spark.read.format('com.github.saurfang.sas.spark').load("../../data/18-83510-I94-Data-2016/i94_jun16_sub.sas7bdat")
    fact_immigration = immigration_spark.distinct().withColumn("immigration_id", monotonically_increasing_id())
    immigration_reshaped = fact_immigration.withColumn("year", col("i94yr").cast("integer"))
    immigration_reshaped = fact_immigration.withColumnRenamed("i94addr", "state_code")
    immigration_reshaped = immigration_reshaped.withColumn("month", col("i94mon").cast("integer"))
    immigration_reshaped = immigration_reshaped.withColumn("city_code", col("i94cit").cast("integer"))
    immigration_reshaped = immigration_reshaped.withColumn("origin_country_code", col("i94res").cast("integer"))
    immigration_reshaped = immigration_reshaped.withColumnRenamed("i94port", "port_code")
    immigration_reshaped = immigration_reshaped.withColumn("data_base_sas", to_date(lit("01/01/1960"), "MM/dd/yyyy"))
    immigration_reshaped = immigration_reshaped.withColumn("arrival_date", expr("date_add(data_base_sas, arrdate)"))
    immigration_reshaped = immigration_reshaped.withColumn("mode_code", col("i94mode").cast("integer"))
    immigration_reshaped = immigration_reshaped.withColumn("departure_date", expr("date_add(data_base_sas, depdate)"))
    immigration_reshaped = immigration_reshaped.withColumn("age", col("i94bir").cast("integer"))
    immigration_reshaped = immigration_reshaped.withColumn("visa_code", col("i94visa").cast("integer"))
    immigration_reshaped = immigration_reshaped.withColumn("birth_year", col("biryear").cast("integer"))
    immigration_reshaped = immigration_reshaped.drop("i94yr", "i94mon", "i94cit", "i94res", "data_base_sas", "arrdate", "i94mode", "depdate", "i94bir", "i94visa", "biryear")
    # Delete the following columns as each of them have only 1 unique value
    immigration_reshaped.drop("validres", "delete_days", "delete_mexl", "delete_dup", "delete_visa", "delete_recdup")
    immigration_reshaped.write.mode("overwrite").parquet("{}{}".format(output, write_to_path["immigration"]))
    logging.info("Immigration data has been uploaded to S3")
    
    
def process_label_data(output):
    """
    Process SAS label data to get five dimension tables countries, cities, mode, state and visa and then write  to S3 as parqueet files
    
    Arguments:
        output: Target S3 endpoint
        
    Returns:
        None
    """
    
    # Read the SAS label file first
    with open("I94_SAS_Labels_Descriptions.SAS") as file:
        auxiliary_data = file.readlines()
    
    # Processing Countries data
    logging.info("Dimension table named Countries data is now being read and it will now be uploaded to S3")
    country = {}
    for countries in auxiliary_data[9:298]:
        line = countries.split("=")
        code = line[0].strip()
        country_name = line[1].strip().strip("'")
        country[code] = country_name
    country_pd = pd.DataFrame(list(country.items()), columns = ['code', 'country'])
    # Write the data to the directory
    country_pd.to_csv("countries.csv", index = False)
    # Convert to Spark dataframe
    country_spark = spark.createDataFrame(country_pd)
    # Write to S3 as parquet files
    country_spark.write.mode("overwrite").parquet("{}{}".format(output, write_to_path["countries"]))
    logging.info("Countries data has been uploaded to S3")
    logging.info()
    logging.info()
    
    # Processing City data
    logging.info("Dimension table named countries is now being read and it will now be uploaded to S3")
    city = {}
    for cities in auxiliary_data[302:962]:
        line = cities.split("=")
        code = line[0].strip().strip("'")
        city_name = line[1].strip().strip("'")
        city[code] = city_name
    city_pd = pd.DataFrame(list(city.items()), columns = ['code', 'city'])
    # Write the data to the directory
    city_pd.to_csv("cities.csv", index = False)
    # Convert to Spark dataframe
    city_spark = spark.createDataFrame(city_pd)
    # Write to S3 as parquet files
    city_spark.write.mode("overwrite").parquet("{}{}".format(output, write_to_path["city"]))
    logging.info("Cities data have been uploaded to S3")
    logging.info()
    logging.info()
    
    # Processing mode data
    logging.info("Dimension table named mode is now being read and it will now be uploaded to S3")
    mode = {}
    for modes in auxiliary_data[972:976]:
        line = modes.split("=")
        code = line[0].strip()
        mode_name = line[1].strip().strip("'").strip(";").strip("'")
        mode[code] = mode_name
    mode_pd = pd.DataFrame(list(mode.items()), columns = ['code', 'mode'])
    # Write the data to the directory
    mode_pd.to_csv("mode.csv", index = False)
    # Convert to Spark dataframe
    mode_spark = spark.createDataFrame(mode_pd)
    # Write to S3 as parquet files
    mode_spark.write.mode("overwrite").parquet("{}{}".format(output, write_to_path["mode"]))
    logging.info("mode data has been uploaded to S3")
    logging.info()
    logging.info()
    
    # Processing state data
    logging.info("Dimension table named state is now being read and it will now be uploaded to S3")
    state = {}
    for states in auxiliary_data[981:1036]:
        line = states.split("=")
        code = line[0].strip().strip("'")
        state_name = line[1].strip().strip("'")
        state[code] = state_name
    state_pd = pd.DataFrame(list(state.items()), columns = ['code', 'state'])
    # Write the data to the directory
    state_pd.to_csv("state.csv", index = False)
    # Convert to Spark dataframe
    state_spark = spark.createDataFrame(state_pd)
    # Write to S3 as parquet files
    state_spark.write.mode("overwrite").parquet("{}{}".format(output, write_to_path["state"]))
    logging.info("state data has been uploaded to S3")
    logging.info()
    logging.info()
    
    # Processing visa data
    logging.info("Dimension table named visa is now being read and it will now be uploaded to S3")
    visa = {}
    for visas in auxiliary_data[1046:1049]:
        line = visas.split("=")
        code = line[0].strip()
        visa_name = line[1].strip()
        visa[code] = visa_name
    visa_pd = pd.DataFrame(list(visa.items()), columns = ['code', 'visa'])
    # Write the data to the directory
    visa_pd.to_csv("visa.csv", index = False)
    # Convert to Spark dataframe
    visa_spark = spark.createDataFrame(visa_pd)
    # Write to S3 as parquet files
    visa_spark.write.mode("overwrite").parquet("{}{}".format(output, write_to_path["visa"]))
    logging.info("visa data has been uploaded to S3")
    logging.info()
    logging.info()
    
    
def process_temperature_data(output):
    """
    Process world temperature data to get one dimension table temperature and then write to S3 as parqueet files
    
    Arguments:
        output: Target S3 endpoint
        
    Returns:
        None
    """
    
    logging.info("Creating table named temperature by reading a CSV file and then process it and upload as parquet files to S3")
    # Read the data
    file_loc = '../../data2/GlobalLandTemperaturesByCity.csv'
    temp_data = pd.read_csv(file_loc)
    # Subset to USA
    temp_data = temp_data[temp_data['Country'] == 'United States']
    
    # Now, extract year and month from the first column dt
    temp_data['dt'] = pd.to_datetime(temp_data['dt'])
    temp_data['year'] = temp_data['dt'].apply(lambda t: t.year)
    temp_data['month'] = temp_data['dt'].apply(lambda t: t.month)
    
    # Change City and Country column values to match the upper case values stored in cities.csv and state.csv
    temp_data['City'] = temp_data['City'].str.upper()
    temp_data['Country'] = temp_data['Country'].str.upper()
    
    # Write the data to the local directory
    temp_data.to_csv("temperature.csv", index = False)
    
    # Write as parquet files the temperature table
    temp_spark = spark.createDataFrame(temp_data) # Convert to Spark dataframe
    temp_spark.write.mode("overwrite").parquet("{}{}".format(output, write_to_path["temp"]))
    logging.info("temperature data has been uploaded to S3")
    logging.info()
    logging.info()
    


def process_demographic_data(output):
    """
    Process the demographic data to get one dimension table temperature and then write to S3 as parqueet files
    
    Arguments:
        output: Target S3 endpoint
        
    Returns:
        None
    """
    
    logging.info("Creating table named demography by reading a CSV file and then process it and upload as parquet files to S3")
    # Read the data
    demographic = pd.read_csv('us-cities-demographics.csv', delimiter=';')
    
    # Changing City and State column values to match the upper case values stored in cities.csv and state.csv
    demographic['City'] = demographic['City'].str.upper()
    demographic['State'] = demographic['State'].str.upper()
    
    # Write the data to the directory
    demographic.to_csv("demographic.csv", index = False)
    
    # Write as parquet files the temperature table
    demographic_spark = spark.createDataFrame(demographic) # Convert to Spark dataframe
    demographic_spark.write.mode("overwrite").parquet("{}{}".format(output, write_to_path["demographic"]))
    logging.info("demographic data has been uploaded to S3")
    logging.info()
    logging.info()
    

def main():
    # Destination of the output directory or the location of the S3 bucket
    output = DEST_S3_BUCKET 
    # First process immigration data and upload it to S3 -- this is the fact table
    process_immigration_data(output)
    # Process all the diffferent data, here 5 tables or dimension tables generated from the SAS label file
    process_label_data(output)
    # Process temperature data -- another dimension table
    process_temperature_data(output)
    # Process demographic data -- another dimension table
    process_demographic_data(output)

    