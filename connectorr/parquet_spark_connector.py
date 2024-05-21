from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import json

spark = SparkSession.builder \
    .appName("ParquetToElasticSearch") \
    .getOrCreate()

# Parquet schema definition
schema = StructType([
    StructField("station_id", LongType(), True),
    StructField("s_no", LongType(), True),
    StructField("battery_status", StringType(), True),
    StructField("status_timestamp", LongType(), True),
    StructField("weather", StructType([
        StructField("humidity", IntegerType(), True),
        StructField("temperature", IntegerType(), True),
        StructField("wind_speed", IntegerType(), True)
    ]), True)
])


stream_data = spark.readStream \
    .format("parquet") \
    .schema(schema) \
    .load("/app/weather/")

# Initialize Elasticsearch client
es = Elasticsearch([{'host': 'elasticsearch', 'port': 9200, 'scheme': 'http'}])


def write_to_es(batch_df, batch_id):

    data = batch_df.toJSON().map(lambda j: json.loads(j)).collect()


    def generate_docs(data):
        for doc in data:
            yield {
                "_index": "weather_data",
                "_source": doc
            }

    # Bulk index data to ElasticSearch
    bulk(es, generate_docs(data))

# Write stream data to Elasticsearch using foreachBatch
query = stream_data.writeStream \
    .foreachBatch(write_to_es) \
    .start()

# Await termination
query.awaitTermination()
