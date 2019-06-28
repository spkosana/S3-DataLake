
# Project Name: Data Lake

###### <H2> Project Overview: </H2>
<p>
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. 
</p>

###### <H2> Project Aim: </H2>
Sparkify like to hire a data engineer to create ETL pipeline that extracts their data from S3, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. The role of a data engineer is mainly to determine what data model to use and create a data model specific database schema and also appropriate ETL pipeline to bring the data from json files in s3 and what framework to use for processing huge amount of data and finally placing the processed data into appropriate data model in S3 for sparkify to do their required analysis. 

###### <H2> Project Description </H2>
After thoroughly reading through the requirement and understanding the data and needs of sparkify,for analysis team to understand the user activity on the app they need necessary statistics of the activities and the results that are retrieved should be fast and accurate. The primary reason dimensional modeling is its ability to allow data to be stored in a way that is optimized for information retrieval once it has been stored in a database.Dimensional models are specifically designed for optimized for the delivery of reports and analytics.It also provides a more simplified structure so that it is more intuitive for business users to write queries. Tables are de-normalized and are few tables which will have few joins to get the results with high performance. 

###### <H2 Dimensional Modelling:></H2>
A dimensional model is also commonly called a star schema.The core of the star schema model is built from fact tables and dimension tables. It consists, typically, of a large table of facts (known as a fact table), with a number of other tables surrounding it that contain descriptive data, called dimensions. 

###### <H2> Fact Table: 
The fact table contains numerical values of what you measure. Each fact table contains the keys to associated dimension tables. Fact tables have a large number of rows.The information in a fact table has characteristics. It is numerical and used to generate aggregates and summaries. All facts refer directly to the dimension keys. Fact table that is determined after carefull analysis which contains the information.Fact table will have data where page column listed as "NextSong" 

###### <H2> Tables (Facts)
Table Name: Songplay(fact)
Column Names: songplay_id, start_time, userId, level, song_id, artist_id, session_id, location, user_agent


###### <H2> Dimension Tables: 
The dimension tables contain descriptive information about the numerical values in the fact table. 

###### <H2> Tables ( Dimensions )

Table Name:Songs (Song Dimension)
Column Names: song_id, title, artist_id, year, duration

Table Name: Artists (Artist Dimension)
Column Names: artist_id, name, location, lattitude, longitude

Table Name:Users (User Dimension)
Column Names: userId, firstName, lastName, gender, level

Table Name: Time (Time Dimension)
Column Names: start_time, hour, day, week, month, year, weekday


###### <H2> Data Processing Approach :
As per the scenarios of data being in the cloud and as the destination is the S3, I have choosen Apache Spark an open-source distributed general purpose cluster computing framework that can do ETL as my data processing methodology to connect to S3 to get the source data into spark dataframe and model the data according to the appropriate tables. Place the user credentials in the dwh.cfg file which helps in connecting to the cloud for accessing data(song_data, log_data).
etl.py script created the spark session object and passes it to the functions listed and read the data into dataframes and get the appropriate columns and put them into parquet file at the defined output bucket in S3. 

ETL Pipeline Script:
etl.py: This is the script that is executed which grabs each file and process it and loads data into songs, artists, users dimensions. Then the time dimension is being loaded using the time stamp column. Fact tables are loaded finally to the complete the whole loading process.

# SQL queries for Analysis: 

Scenario 1: Sparkify want to analyse how many users are paid and free please find the Query to get the results. 

spark.sql(
        """SELECT u.level, count(distinct u.userId) Account_type  
        FROM songplays sp join users u on sp.userId = u.userId 
        group by u.level"""
).show()

Scenario 2: sparkify want to analyse how many times the user accessed the apps, to get the activity count please use below query

spark.sql(
         """
         SELECT u.level, u.firstName, u.lastName , count(distinct sessionId) user_activity  
         FROM songplays sp join users u on sp.userId = u.userId 
         group by u.level,u.firstName,u.lastName
         order by level,firstName,lastName
         """).show()


Scenario 3 : sparkify want to analyse how many uses are accessing the app on the monthly and weekly basis. 

spark.sql(
         """
       SELECT t.month , count(distinct u.userId) user_activity  
        FROM songplays sp 
        join users u on sp.userId = u.userId  
        join time t on sp.start_time = t.start_time 
        group by t.month
         """).show()


spark.sql(
         """
        SELECT t.month,t.week , count(distinct u.userId) user_activity  
        FROM songplays sp 
          join users u on sp.userId = u.userId  
          join time t on sp.start_time = t.start_time 
        group by t.month,t.week
        Order by t.month,t.week
         """).show()





