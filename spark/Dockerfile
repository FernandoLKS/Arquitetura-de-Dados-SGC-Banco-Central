FROM apache/spark:3.5.3

USER root

RUN pip install --no-cache-dir \
    python-dotenv \
    delta-spark==3.2.0

RUN mkdir -p /opt/spark/jars

# Hadoop AWS / S3A
RUN curl -fL \
    -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

RUN curl -fL \
    -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# PostgreSQL JDBC
RUN curl -fL \
    -o /opt/spark/jars/postgresql-42.7.4.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.4.jar

# Delta Lake
RUN curl -fL \
    -o /opt/spark/jars/delta-spark_2.12-3.2.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar

RUN curl -fL \
    -o /opt/spark/jars/delta-storage-3.2.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar

RUN ls -lh /opt/spark/jars/

USER spark