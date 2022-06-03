import os
from schema import schema
from utils import create_session, read_stream, process_and_write_stream

"""
Run the script using the following command:

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 stream_all_events.py

"""

KAFKA_IP_ADDRESS = os.getenv("KAFKA_ADDRESS", "localhost")
KAFKA_PORT = 9092
STORAGE_PATH = "../output"
TRIGGER_PERIOD = "10 seconds"

spark = create_session("Eventsim")
spark.streams.resetTerminated()

streams = []
kafka_topics = ["auth_events"]

for kafka_topic in kafka_topics:
    streams.append(read_stream(spark, KAFKA_IP_ADDRESS, KAFKA_PORT, kafka_topic))

print("ADFASDFASDFASDFASDFASDFASDF", streams[0])

for stream, kafka_topic in zip(streams, kafka_topics):
    process_and_write_stream(
        stream,
        stream_schema=schema[kafka_topic],
        topic_name=kafka_topic,
        file_format="parquet",
        storage_path=f"{STORAGE_PATH}/{kafka_topic}",
        checkpoint_path=f"{STORAGE_PATH}/checkpoint/{kafka_topic}",
        trigger=TRIGGER_PERIOD,
        output_mode="append",
    )

# spark.streams.awaitAnyTermination()
