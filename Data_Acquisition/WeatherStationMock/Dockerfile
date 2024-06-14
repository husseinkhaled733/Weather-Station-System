FROM openjdk:17-jdk-alpine
WORKDIR /app
COPY target/WeatherStationMock-1.0-SNAPSHOT-jar-with-dependencies.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]