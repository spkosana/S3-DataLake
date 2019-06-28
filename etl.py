import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,to_timestamp,dayofweek,from_unixtime


config = configparser.ConfigParser()
config.read('dl.cfg')

# Getting Access key and secret key from the config file. 
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Aim:    To create and return spark session 
    Input:  NA
    Output: Returns spark session. 
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Aim:    To read the json files from s3 and write the related columns to songs, artists
    Input:  spark, input_data(source files url), output_data(target files url)
    Output: songs, artists will be written to given target url.    
    """
    # get filepath to song data file
    song_data = input_data+"song_data/*/*/*/*"
     
        
    # read song data file
    global song_df
    song_df = spark.read.json(song_data)

    # extract columns to create songs table
    fcols = ["song_id", "title", "artist_id", "year", "duration"]    
    songs_table = song_df[fcols].dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data,"songs"), "overwrite")
    

    # extract columns to create artists table
    afcols = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    artists_table = song_df[afcols].dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data,"artists"),"overwrite")
    


def process_log_data(spark, input_data, output_data):
    """
    Aim:    To read the json files from s3 and write the related columns to users, time and songplays
    Input:  spark, input_data(source files url), output_data(target files url)
    Output: users, time,songplays will be written to given target url.    
    """
    # get filepath to log data file
    log_data = input_data+"log_data/*/*/*"

    # read log data file
    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    log_df = log_df[log_df['page']=='NextSong']

    # extract columns for users table 
    ufcols = ["userId", "firstName", "lastName", "gender", "level"]
    users_table = log_df[ufcols].dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data,"users"),"overwrite")
    

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.fromtimestamp(x/1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    time_table = log_df[["ts"]]
    time_table = time_table.withColumn("start_time", to_timestamp(get_timestamp(time_table.ts)))\
                      .withColumn("hour", hour(to_timestamp(get_timestamp(time_table.ts))))\
                      .withColumn("day", dayofmonth(to_timestamp(get_timestamp(time_table.ts))))\
                      .withColumn("week", weekofyear(to_timestamp(get_timestamp(time_table.ts))))\
                      .withColumn("month", month(to_timestamp(get_timestamp(time_table.ts))))\
                      .withColumn("year", year(to_timestamp(get_timestamp(time_table.ts))))\
                      .withColumn("weekday", dayofweek(to_timestamp(get_timestamp(time_table.ts))))
    time_table = time_table[["start_time", "hour", "day", "week", "month", "year", "weekday"]].dropDuplicates()

 
   
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data,"time"),"overwrite")
    

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df.join(log_df, (song_df.title == log_df.song) & (song_df.duration == log_df.length) & (song_df.artist_name == log_df.artist ))
    songplays_table  = songplays_table.withColumn("songplay_id",monotonically_increasing_id())
    songplays_table = songplays_table[['songplay_id',from_unixtime((songplays_table.ts.cast('bigint')/1000)).cast('timestamp').alias('start_time'),'userId','level','song_id','artist_id','sessionId','artist_location','userAgent']]
    songplays_table = songplays_table.withColumn("year",year("start_time")).withColumn("month",month("start_time"))
    

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(os.path.join(output_data,"songplays"),"overwrite")



def main():
    spark = create_spark_session()
    input_data  = "s3a://udacity-dend/"
    output_data = "s3a://output_data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
