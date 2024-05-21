#!/bin/bash

# Start Elasticsearch in the background
/usr/share/elasticsearch/bin/elasticsearch &

# Wait for Elasticsearch to start
echo "Waiting for Elasticsearch to start..."
until curl -s http://localhost:9200; do
  sleep 1
done

sleep 10

# Create the index with mappings using curl
curl -XPUT 'http://localhost:9200/weather_data' -H 'Content-Type: application/json' -d '{
  "mappings": {
    "properties": {
      "battery_status": {
        "type": "keyword"
      },
      "s_no": {
        "type": "long"
      },
      "station_id": {
        "type": "long"
      },
      "status_timestamp": {
        "enabled": false
      },
      "timestamp": {
        "enabled": false
      },
      "timestamp<": {
        "enabled": false
      },
      "weather": {
        "properties": {
          "humidity": {
            "enabled": false
          },
          "temperature": {
            "enabled": false
          },
          "wind_speed": {
            "enabled": false
          }
        }
      }
    }
  }
}'

# Keep the container running
tail -f /dev/null