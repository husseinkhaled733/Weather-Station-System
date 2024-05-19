package org.example.parquet;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.example.Utils;
import org.example.dto.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ParquetWriter {

    private final SparkSession sparkSession;
    private final StructType schema;
    private final int batch_size = 10;

    private List<Message> messageList = new ArrayList<>();

    private final Logger log = LoggerFactory.getLogger(ParquetWriter.class);

    public ParquetWriter() {

        // create spark session
        this.sparkSession = SparkSession
                .builder()
                .appName("main")
                .master("local[*]")
                .config("parquet.enable.summary-metadata", "false")
                .config("dfs.client.read.shortcircuit.skip.checksum", "true")
                .getOrCreate();

        // initialize parquet schema
        this.schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("station_id", DataTypes.LongType, false),
                DataTypes.createStructField("s_no", DataTypes.LongType, false),
                DataTypes.createStructField("battery_status", DataTypes.StringType, false),
                DataTypes.createStructField("status_timestamp", DataTypes.LongType, false),
                DataTypes.createStructField("timestamp>", DataTypes.LongType, false), // This field is added temporary to help in partitioning
                DataTypes.createStructField("weather", DataTypes.createStructType(new StructField[]{
                        DataTypes.createStructField("humidity", DataTypes.IntegerType, false),
                        DataTypes.createStructField("temperature", DataTypes.IntegerType, false),
                        DataTypes.createStructField("wind_speed", DataTypes.IntegerType, false)
                }), true)
        });
    }

    public void addMessagesUtil(ConsumerRecords<String, String> messages) {

        // extract messages
        List<Message> currBatch = messagesDecoder(messages);

        // add the current batch to the main batch, this function returns a reference to the main batch if it reached the max size
        List<Message> batch = addMessages(currBatch);
        if (batch != null) {
            List<Row> listRows = batch.stream()
                    .map(Message::toRow)
                    .collect(Collectors.toList());

            Dataset<Row> df = sparkSession.createDataFrame(listRows, schema);

            df.write()
                    .mode("append")
                    .partitionBy("timestamp>", "station_id").parquet("Base-Central-Station/weather_data");
        }
    }

    private List<Message> messagesDecoder(ConsumerRecords<String, String> messages) {

        // convert consumed string messages to Message class
        List<Message> currBatch = new ArrayList<>();
        messages.forEach((msg) ->
        {
            try {
                currBatch.add(Utils.objectMapper.readValue(msg.value(), Message.class));
            } catch (JsonProcessingException e) {
                //  Can be added to the invalid letter channel
                log.atWarn().log("Invalid message sent");
            }
        });
        return currBatch;
    }

    private synchronized List<Message> addMessages(List<Message> message) {

        // add messages to messageList (which holds current batch to be written)
        // note that Lists are not thread safe
        // the options where either to choose a synchronized list or manually lock the normal list,
        // concerning the first option we will still have to check the size and clear the current accumulative list atomically

        messageList.addAll(message);
        if (messageList.size() >= batch_size) {
            List<Message> tempMessageList = messageList;
            messageList = new ArrayList<>();
            return tempMessageList;
        }
        return null;
    }
}


