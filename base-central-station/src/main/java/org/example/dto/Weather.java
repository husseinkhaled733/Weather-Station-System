package org.example.dto;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.io.Serializable;



@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Weather implements Serializable {
    int humidity;
    int temperature;
    int wind_speed;

    public String toString() {
        return "{ \"humidity\": " + humidity + ",\n" + "\"temperature\": " + temperature + ",\n" + "\"wind_speed\": " + wind_speed + "\n}";
    }
    public Row toRow() {
        return RowFactory.create(humidity, temperature, wind_speed);
    }
}
