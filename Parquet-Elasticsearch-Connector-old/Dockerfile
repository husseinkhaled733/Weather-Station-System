FROM spark:3.5.1-scala2.12-java11-ubuntu

COPY target/*.jar .

ENTRYPOINT ["java", "-jar", "Parquet-Elasticsearch-Connector-1.0-SNAPSHOT-jar-with-dependencies.jar"]
