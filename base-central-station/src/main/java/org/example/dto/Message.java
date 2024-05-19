package org.example.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.io.*;
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Message implements Serializable {

    long station_id;
    long s_no;
    String battery_status;
    long status_timestamp;
    Weather weather;

    @Override
    public String toString() {
        return "{" + "\n\"station_id\": " + station_id + ",\n\"s_no\": " + s_no + ",\n" + "\"battery_status\": " + "\"" + battery_status + "\"" + ",\n\"status_timestamp\": " + status_timestamp + ",\n\"weather\": " + weather.toString() + "\n}";
    }

    public Row toRow() {
        return RowFactory.create(station_id, s_no, battery_status, status_timestamp, status_timestamp - (status_timestamp % 3600), weather.toRow());
    }

}
