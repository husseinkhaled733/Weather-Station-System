package org.example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

public class ParquetElasticSearchConnector {
    public static void main(String[] args) throws StreamingQueryException {

        // spark session initialization
        SparkSession spark = SparkSession
                .builder()
                .appName("main")
                .master("local[*]")
                .config("spark.es.index.auto.create", "true")
                .config("spark.sql.streaming.checkpointLocation", "/checkpoint/")
                .config("spark.hadoop.fs.defaultFS", "file:///")
                .config("spark.es.nodes","es01-test")
                .config("spark.es.port","9200")
                .config("spark.es.nodes.wan.only","true")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // Parquet schema
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("station_id", DataTypes.LongType, true),
                DataTypes.createStructField("s_no", DataTypes.LongType, true),
                DataTypes.createStructField("battery_status", DataTypes.StringType, true),
                DataTypes.createStructField("status_timestamp", DataTypes.LongType, true),
                DataTypes.createStructField("weather", DataTypes.createStructType(new StructField[]{
                        DataTypes.createStructField("humidity", DataTypes.IntegerType, true),
                        DataTypes.createStructField("temperature", DataTypes.IntegerType, true),
                        DataTypes.createStructField("wind_speed", DataTypes.IntegerType, true)
                }), true)
        });

        // data stream from parquet files
        Dataset<Row> streamData = spark
                .readStream()
                .format("parquet")
                .schema(schema)
                .load("/app/weather_data");

        // writing data stream to elasticsearch
        streamData.writeStream()
                .outputMode("append")
                .format("org.elasticsearch.spark.sql")
                .option("es.nodes","elasticsearch")
                .option("es.port", "9200")
                .option("checkpointLocation", "/")
                .start("weather_data/").awaitTermination();
    }
}