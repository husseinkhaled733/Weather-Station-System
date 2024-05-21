FROM spark:3.5.0-java17

WORKDIR /app

RUN mkdir -p /app/bitcask_store && chmod -R 777 /app/bitcask_store

COPY target/*.jar .

COPY src/main/resources/application.properties .

ENTRYPOINT ["java", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", "-jar", "Base-Central-Station-1.0-SNAPSHOT.jar"]