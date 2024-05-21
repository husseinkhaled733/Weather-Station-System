package org.example;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.json.JSONObject;

public class RainDetectorProcessor extends AbstractProcessor<String, String> {

    @Override
    public void process(String key, String value) {
        JSONObject message = new JSONObject(value);
        System.out.println("Processing message: " + message);
        int humidity = message.getJSONObject("weather").getInt("humidity");

        if (humidity > 70) {
            long stationId = message.getLong("station_id");
            long serialNumber = message.getLong("s_no");
            long statusTimestamp = message.getLong("status_timestamp");

            String rainMessage = String.format("{\"station_id\":%d,\"s_no\":%d,\"status_timestamp\":%d,\"raining\":true}",
                    stationId, serialNumber, statusTimestamp);

            context().forward(key, rainMessage);
        }
    }
}
