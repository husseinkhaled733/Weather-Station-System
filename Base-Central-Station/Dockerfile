FROM spark:3.5.0-java17

WORKDIR /app

COPY target/*.jar .

COPY src/main/resources/application.properties .

ENTRYPOINT ["java", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", "-jar", "Base-Central-Station-1.0-SNAPSHOT.jar"]

