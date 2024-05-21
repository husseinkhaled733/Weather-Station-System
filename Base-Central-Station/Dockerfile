FROM spark:3.5.0-java17

# Set the working directory
WORKDIR /app

# Create the directory with correct permissions
RUN mkdir -p /app/bitcask_store && chmod -R 777 /app/bitcask_store

# Copy the application JAR file
COPY target/*.jar /app/

# Copy application properties
COPY src/main/resources/application.properties /app/

# Ensure entrypoint is correct
ENTRYPOINT ["java", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", "-jar", "/app/Base-Central-Station-1.0-SNAPSHOT.jar"]
