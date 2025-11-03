# This Dockerfile is mostly AI generated as bitnami images are
# now behind a paywall. This image takes quite a while to build,
# pushing it to a Docker registry makes a lot of sense.
FROM eclipse-temurin:11

ARG SPARK_VERSION=3.5.7
ARG HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

RUN apt-get update && apt-get install -y curl wget bash \
    && wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && mkdir -p ${SPARK_HOME} \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C ${SPARK_HOME} --strip-components=1 \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Add Hadoop AWS and AWS SDK jars for S3A support
RUN mkdir -p ${SPARK_HOME}/jars \
    && wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -O ${SPARK_HOME}/jars/hadoop-aws.jar \
    && wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.696/aws-java-sdk-bundle-1.12.696.jar -O ${SPARK_HOME}/jars/aws-java-sdk-bundle.jar \
    && wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.0/iceberg-spark-runtime-3.5_2.12-1.6.0.jar -O ${SPARK_HOME}/jars/iceberg-spark-runtime.jar

# Install required packages including Python
RUN apt-get install -y \
    python3 python3-pip \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

# Copy custom startup script and conf
COPY start-spark.sh /start-spark.sh
# COPY conf /opt/spark/conf
RUN chmod +x /start-spark.sh

WORKDIR /opt/spark
EXPOSE 8080 7077 8081

ENTRYPOINT ["/start-spark.sh"]
