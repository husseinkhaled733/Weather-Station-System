from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import json

class ParquetToElasticsearch:

    def __init__(self, es_host, es_port, es_scheme, index_name, index_mapping_file):
        self.es = Elasticsearch([{'host': es_host, 'port': es_port, 'scheme': es_scheme}])
        self.index_name = index_name
        self.index_mapping_file = index_mapping_file
        self.initialize_index()

    def write_to_es(self, batch_df, batch_id):
        data = batch_df.toJSON().map(lambda j: json.loads(j)).collect()

        def generate_docs(data):
            for doc in data:
                yield {
                    "_index": self.index_name,
                    "_source": doc
                }

        # Bulk index data to Elasticsearch
        bulk(self.es, generate_docs(data))

    def initialize_index(self):
        if not self.es.indices.exists(index=self.index_name):
            with open(self.index_mapping_file, 'r') as file:
                mapping = json.load(file)
            self.es.indices.create(index=self.index_name, mappings=mapping.get('mappings'))

    def initialize_parquet_schema(self):
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
        return schema

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ParquetToElasticSearch") \
        .getOrCreate()


    # Initialize Elasticsearch and streaming to Elasticsearch
    parquet_to_es = ParquetToElasticsearch(
        es_host='elasticsearch-service',
        es_port=9200,
        es_scheme='http',
        index_name='weather_data',
        index_mapping_file='index_mapping.json'
    )

    # Parquet schema definition
    schema = parquet_to_es.initialize_parquet_schema()

    stream_data = spark.readStream \
        .format("parquet") \
        .schema(schema) \
        .load("/app/weather/")

    # Write stream data to Elasticsearch
    query = stream_data.writeStream \
        .foreachBatch(parquet_to_es.write_to_es) \
        .start()

    query.awaitTermination()
