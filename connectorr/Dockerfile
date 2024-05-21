# Start with a base image containing Java runtime
FROM openjdk:11

# Install Python
RUN apt-get update \
    && apt-get install -y python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install elasticsearch==8.1.3 pyspark

# Verify installations
RUN java -version
RUN python3 --version
COPY parquet_spark_connector.py .

CMD [ "python3", "parquet_spark_connector.py"]
