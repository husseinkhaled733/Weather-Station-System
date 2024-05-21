#!/bin/bash

/usr/share/kibana/bin/kibana &

echo "Waiting for Kibana to start..."
until curl -s http://localhost:5601; do
  sleep 1
done

sleep 200

echo "Importing dashboards into Kibana..."
until curl -X POST "http://localhost:5601/api/saved_objects/_import" -H "kbn-xsrf: true" --form file=@"/usr/share/kibana/saved_objects/export.ndjson"; do
  sleep 1
done


# Keep the container running
tail -f /dev/null