from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, \
    date_format, monotonically_increasing_id
from pyspark.sql.types import ShortType, TimestampType
import configparser
import os

# Get AWS credentials from Config file
config = configparser.ConfigParser()
config.read("dl.cfg")
# Set as Environment variables
os.environ["AWS_ACCESS_KEY_ID"] = config["AWS"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS"]["AWS_SECRET_ACCESS_KEY"]


def create_spark_session():
    """
    Create Spark Session.
    Function which wraps the creation of a Spark Session object with the
    specified parameters.
    Returns:
        - spark: started Spark Session object.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process Song Data.
    Function to processs song data file given as input, to save as as output
    parquet file.
    Args:
        - spark: started Spark Session object.
        - input_data: string with path to input file.
        - output_data: string with path to output file.
    """
    # get filepath to song data file
    song_data = f"{input_data}song_data/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df\
        .where(col("song_id").isNotNull())\
        .dropDuplicates(["song_id"])\
        .select("song_id", "title", "artist_id", "year", "duration")\
        .withColumn("year", col("year").cast(ShortType()))

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(f"{output_data}songs_table.parquet",
                              mode="overwrite")
    # extract columns to create artists table
    artists_table = df\
        .where(col("artist_id").isNotNull())\
        .dropDuplicates(["artist_id"])\
        .select("artist_id", col("artist_name").alias("name"),
                col("artist_location").alias("location"),
                col("artist_latitude").alias("latitude"),
                col("artist_longitude").alias("longitude"))

    # write artists table to parquet files
    artists_table.write.parquet(f"{output_data}artists_table.parquet",
                                mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Process Log Data.
    Function to processs log data file given as input, to save as as output
    parquet file.
    Args:
        - spark: started Spark Session object.
        - input_data: string with path to input file.
        - output_data: string with path to output file.
    """
    # get filepath to log data file
    log_data = f"{input_data}log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where(col("page") == "NextSong")

    # extract columns for users table
    users_table = df\
        .where(col("userId").isNotNull())\
        .dropDuplicates(["userId"])\
        .select(col("userId").cast("int").alias("user_id"),
                col("firstName").alias("first_name"),
                col("lastName").alias("last_name"), "gender", "level")\

    # write users table to parquet files
    users_table.write.parquet(f"{output_data}users_table.parquet",
                              mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(datetime.fromtimestamp(x/1000)))
    df = df.withColumn("timestamp", get_timestamp("ts"))

    # create datetime column from original timestamp column
    df = df.withColumn("dt", col("timestamp").cast(TimestampType()))

    # extract columns to create time table
    time_table = df\
        .where(col("dt").isNotNull())\
        .dropDuplicates(["dt"])\
        .select(col("dt").alias("start_time"), hour("dt").alias("hour"),
                dayofmonth("dt").alias("day"), weekofyear("dt").alias("week"),
                month("dt").alias("month"),
                year("dt").alias("year").cast(ShortType()),
                date_format("dt", "E").alias("weekday"))

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(f"{output_data}time_table.parquet",
                             mode="overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(f"{output_data}songs_table.parquet")\
        .select("song_id", "artist_id", "title")

    # extract columns from joined song and log datasets to create songplays
    # table
    songplays_table = df\
        .join(song_df, df.song == song_df.title, how="left")\
        .select(monotonically_increasing_id().alias("songplay_id"),
                col("dt").alias("start_time"), col("userId").alias("user_id"),
                "song_id", "artist_id", col("sessionId").alias("session_id"))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(f"{output_data}songplays_table.parquet",
                                  mode="overwrite")


def main():
    """
    Main function.
    Main function to execute when module is called through command line.
    Creates a spark session object, gets data from S3, processes it and saves
    output to another s3 file.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dataengineer-datalake/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
