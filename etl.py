from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month

def create_spark_session():
    """
    New Spark session initialization for the ETL Process
    
    Output:
    SparkSession
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Processing songs and artists data and loading them into parquet files
    
    Input:
    spark : new sparksession for working
    input_data:   the path that we get the data from
    output_data:  the path that we store the parquet files in

    Output:

    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_columns = ['song_id','title','artist_id','year','duration']
    songs_table = df.selectExpr(*songs_columns)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(['year','artist_id']).parquet("{}/songs.parquet".format(output_data),mode="overwrite")

    # extract columns to create artists table
    artists_columns = ['artist_id','artist_name as name','artist_location as location','artist_latitude as latitude','artist_longitude as longitude']

    artists_table = df.selectExpr(*artists_columns)
    
    artists_table.createOrReplaceTempView('artists')
    
    artists_table = spark.sql("select artist_id, \
                  name, \
                  case \
                      when trim(location) = '' \
                      then Null \
                      else location \
                      end as location, \
                  latitude,longitude \
                  from artists")
    
    # write artists table to parquet files
    artists_table.write.parquet("{}/artists.parquet".format(output_data),mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Processing log data to transform it into users dimension ,time dimension and songplays transaction grain fact table
    then loading them into parquet files

    Input:
    spark : new sparksession for working
    input_data:   the path that we get the data from
    output_data:  the path that we store the parquet files in

    Output:

    """
    # get filepath to log data file
    log_data = input_data+'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df = df.filter(df.page =='NextSong')

    # extract columns for users table
    users_columns = ['userId as user_id','firstName as first_name','lastName as last_name','gender','level']
    users_table = df.selectExpr(*users_columns).distinct()
    
    # write users table to parquet files
    users_table.write.parquet('{}/users.parquet'.format(output_data),mode="overwrite")

    df.createOrReplaceTempView("user_logs")
    # extract columns to create time table
    time_table = spark.sql("select \
                        t.timestamp as start_time, \
                        hour(t.timestamp)       as hour, \
                        day(t.timestamp)        as day, \
                        weekofyear(t.timestamp) as week, \
                        month(t.timestamp)      as month, \
                        year(t.timestamp)       as year, \
                        dayofweek(t.timestamp)  as weekday \
                        from \
                        (select from_unixtime(ts/1000) as timestamp from user_logs group by timestamp) as t"
                        )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(['year','month']).parquet("{}/time.parquet".format(output_data),mode="overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.parquet("{}/songs.parquet".format(output_data))

    song_df.createOrReplaceTempView("songs")
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("select \
                             monotonically_increasing_id() as songplay_id, \
                             from_unixtime(user_logs.ts/1000) as start_time, \
                             user_logs.userId as user_id, \
                             user_logs.level as level,\
                             songs.song_id as song_id, \
                             songs.artist_id as artist_id, \
                             user_logs.sessionId as session_id, \
                             user_logs.location, \
                             user_logs.userAgent as user_agent\
                             from \
                             user_logs \
                             left join songs \
                             on user_logs.song = songs.title"
                             )

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.withColumn('year',year(songplays_table.start_time)).withColumn('month',month(songplays_table.start_time))
    
    songplays_table.write.partitionBy(['year','month']).parquet("{}/songplays.parquet".format(output_data),mode="overwrite")


def main():
    """
    The main function which creates the sparksession and execute the etl process till the end
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-etl/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

    spark.stop()


if __name__ == "__main__":
    main()
