from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_json, year, month, hour, dayofmonth


def create_session(app_name, master="yarn"):
    return (
        SparkSession.builder
        .appName(app_name)  # .master(master)
        .master("local")
        .getOrCreate()
    )


def read_stream(spark, kafka_ip_address, kafka_port, kafka_topic):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", f"{kafka_ip_address}:{kafka_port}")
        .option("failOnDataLoss", False)
        .option("startingOffsets", "earliest")
        .option("subscribe", kafka_topic)
        .load()
    )


def process_stream(stream, stream_schema, topic_name):
    stream = (
        stream.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), stream_schema).alias("data"))
        .select("data.*")
    )

    stream = (
        stream.withColumn("ts", (col("ts") / 1000).cast("timestamp"))
        .withColumn("year", year(col("ts")))
        .withColumn("month", month(col("ts")))
        .withColumn("hour", hour(col("ts")))
        .withColumn("day", dayofmonth(col("ts")))
    )

    if topic_name in ["listen_events", "page_view_events"]:
        stream = (
            stream.withColumn("song", string_decode("song"))
            .withColumn("artist", string_decode("artist"))
        )

    return stream


def write_stream(stream, file_format, storage_path, checkpoint_path, trigger, output_mode):
    return (
        stream.writeStream
        .format(file_format)
        #.partitionBy("month", "day", "hour")
        .option("path", storage_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger)
        .outputMode(output_mode)
    )


def process_and_write_stream(
    stream, stream_schema, topic_name, file_format, storage_path, checkpoint_path, trigger, output_mode
):
    stream = process_stream(stream, stream_schema, topic_name)
    print("done process!!!!!!!!!!!!!!!!!!!!!!!")
    stream = write_stream(stream, file_format, storage_path, checkpoint_path, trigger, output_mode)
    stream.start()


@udf
def string_decode(s, encoding="utf-8"):
    if s:
        return (
            s.encode("latin1")
            .decode("unicode-escape")
            .encode('latin1')
            .decode(encoding)
            .strip('"')
        )
    return s
