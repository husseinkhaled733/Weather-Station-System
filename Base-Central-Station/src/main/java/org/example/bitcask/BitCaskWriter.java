package org.example.bitcask;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.example.BitCask;
import org.example.Utils;
import org.example.dto.Message;
import org.example.models.Options;
import org.example.parquet.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BitCaskWriter {

    private final BitCask<RecordId, Message> bitCask;
    private final Logger log = LoggerFactory.getLogger(ParquetWriter.class);

    public BitCaskWriter() {
        this.bitCask = new BitCask<>(Options.builder()
                .baseDir("/app/bitcask_store/")
                .build());
    }

    public void writeRecords(ConsumerRecords<String, String> messages) {
        messages.forEach(
                record -> {
                    try {
                        Message message = Utils.objectMapper.readValue(record.value(), Message.class);
                        RecordId recordId = new RecordId(message.getStation_id(), message.getS_no());

                        bitCask.put(recordId , message);
                    } catch (JsonProcessingException e) {
                        log.atWarn().log("Invalid message sent");
                    }
                }
        );
    }

}
