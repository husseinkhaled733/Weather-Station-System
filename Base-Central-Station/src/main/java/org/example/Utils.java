package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.slf4j.Log4jLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class Utils {

    public static final String propertiesPath = "application.properties";
    public static final String TOPIC = "weather_data";
    public static final ObjectMapper objectMapper = new ObjectMapper();

    public static Properties loadConfig(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }

        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }
        return cfg;
    }
}