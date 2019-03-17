FROM alpine:3.9
# A few reasons for installing distribution-provided OpenJDK:
#
#  1. Oracle.  Licensing prevents us from redistributing the official JDK.
#
#  2. Compiling OpenJDK also requires the JDK to be installed, and it gets
#     really hairy.
#
#     For some sample build times, see Debian's buildd logs:
#       https://buildd.debian.org/status/logs.php?pkg=openjdk-8

# Default to UTF-8 file.encoding
ENV LANG C.UTF-8

# add a simple script that can auto-detect the appropriate JAVA_HOME value
# based on whether the JDK or only the JRE is installed
RUN { \
  echo '#!/bin/sh'; \
  echo 'set -e'; \
  echo; \
  echo 'dirname "$(dirname "$(readlink -f "$(which javac || which java)")")"'; \
  } > /usr/local/bin/docker-java-home \
  && chmod +x /usr/local/bin/docker-java-home
ENV JAVA_HOME /usr/lib/jvm/java-1.8-openjdk
ENV PATH $PATH:/usr/lib/jvm/java-1.8-openjdk/jre/bin:/usr/lib/jvm/java-1.8-openjdk/bin

ENV JAVA_ALPINE_VERSION 8.191.12-r0

RUN set -x \
  && apk add --no-cache \
  openjdk8="$JAVA_ALPINE_VERSION" \
  && [ "$JAVA_HOME" = "$(docker-java-home)" ]

# Spark Verison
ENV SPARK_VERSION         2.4.0
ENV HADOOP_VERSION        2.7
ENV SPARK_HOME            /opt/spark
ENV SPARK_JARS            /opt/spark/jars/
ENV SPARK_CHECKSUM_URL    https://www.apache.org/dist/spark
ENV SPARK_DOWNLOAD_URL    https://www-us.apache.org/dist/spark
ENV GLIBC_VERSION         2.26-r0

# install spark
RUN set -ex && \
  apk upgrade --update && \
  apk add --update libstdc++ ca-certificates bash openblas findutils coreutils && \
  for pkg in glibc-${GLIBC_VERSION} glibc-bin-${GLIBC_VERSION} glibc-i18n-${GLIBC_VERSION}; do wget https://github.com/andyshinn/alpine-pkg-glibc/releases/download/${GLIBC_VERSION}/${pkg}.apk -O /tmp/${pkg}.apk; done && \
  apk add --allow-untrusted /tmp/*.apk && \
  rm -v /tmp/*.apk && \
  ( /usr/glibc-compat/bin/localedef --force --inputfile POSIX --charmap UTF-8 C.UTF-8 || true ) && \
  echo "export LANG=C.UTF-8" > /etc/profile.d/locale.sh && \
  /usr/glibc-compat/sbin/ldconfig /lib /usr/glibc-compat/lib && \
  mkdir -p ${SPARK_HOME} && \
  wget -O spark.sha ${SPARK_CHECKSUM_URL}/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz.sha512 && \
  export SPARK_SHA512_SUM=$(grep -o "[A-F0-9]\{8\}" spark.sha | awk '{print}' ORS='' | tr '[:upper:]' '[:lower:]') && \
  rm -f spark.sha && \
  wget -O spark.tar.gz ${SPARK_DOWNLOAD_URL}/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
  echo "${SPARK_SHA512_SUM}  spark.tar.gz" | sha512sum -c - && \
  gunzip -c spark.tar.gz | tar -xf - -C $SPARK_HOME --strip-components=1 && \
  rm -f spark.tar.gz

# spark extensions
RUN wget -P ${SPARK_JARS} https://repo.maven.apache.org/maven2/com/databricks/spark-xml_2.11/0.5.0/spark-xml_2.11-0.5.0.jar && \    
  wget -P ${SPARK_JARS} https://repo.maven.apache.org/maven2/org/apache/spark/spark-avro_2.11/2.4.0/spark-avro_2.11-2.4.0.jar && \
  # aws hadoop
  wget -P ${SPARK_JARS} https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/2.7.4/hadoop-aws-2.7.4.jar && \
  wget -P ${SPARK_JARS} https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk/1.11.197/aws-java-sdk-1.11.197.jar && \
  # azure hadoop
  wget -P ${SPARK_JARS} https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-azure/2.7.4/hadoop-azure-2.7.4.jar && \
  wget -P ${SPARK_JARS} https://repo.maven.apache.org/maven2/com/microsoft/azure/azure-storage/3.1.0/azure-storage-3.1.0.jar && \   
  # azure datalake store 
  wget -P ${SPARK_JARS} http://repo.hortonworks.com/content/repositories/releases/org/apache/hadoop/hadoop-azure-datalake/2.7.3.2.6.5.3000-28/hadoop-azure-datalake-2.7.3.2.6.5.3000-28.jar && \
  wget -P ${SPARK_JARS} http://central.maven.org/maven2/com/microsoft/azure/azure-data-lake-store-sdk/2.3.1/azure-data-lake-store-sdk-2.3.1.jar && \
  # kafka
  wget -P ${SPARK_JARS} https://repo.maven.apache.org/maven2/org/apache/kafka/kafka_2.11/1.1.0/kafka_2.11-1.1.0.jar && \   
  wget -P ${SPARK_JARS} https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/1.1.0/kafka-clients-1.1.0.jar && \   
  wget -P ${SPARK_JARS} https://repo.maven.apache.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.11/2.3.1/spark-sql-kafka-0-10_2.11-2.3.1.jar && \
  # azure eventhub
  wget -P ${SPARK_JARS} https://repo.maven.apache.org/maven2/com/microsoft/azure/azure-eventhubs/1.2.0/azure-eventhubs-1.2.0.jar && \       
  wget -P ${SPARK_JARS} https://repo.maven.apache.org/maven2/org/apache/qpid/proton-j/0.29.0/proton-j-0.29.0.jar && \   
  # databases
  wget -P ${SPARK_JARS} https://repository.mulesoft.org/nexus/content/repositories/public/com/amazon/redshift/redshift-jdbc4/1.2.10.1009/redshift-jdbc4-1.2.10.1009.jar && \
  wget -P ${SPARK_JARS} https://repo.maven.apache.org/maven2/com/microsoft/sqlserver/mssql-jdbc/7.2.1.jre8/mssql-jdbc-7.2.1.jre8.jar && \
  wget -P ${SPARK_JARS} https://repo.maven.apache.org/maven2/com/microsoft/azure/azure-sqldb-spark/1.0.2/azure-sqldb-spark-1.0.2.jar && \
  wget -P ${SPARK_JARS} https://jdbc.postgresql.org/download/postgresql-42.2.2.jar && \  
  wget -P ${SPARK_JARS} https://repo.maven.apache.org/maven2/com/datastax/spark/spark-cassandra-connector_2.11/2.0.5/spark-cassandra-connector_2.11-2.0.5.jar && \
  wget -P ${SPARK_JARS} https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/5.1.45/mysql-connector-java-5.1.45.jar && \ 
  wget -P ${SPARK_JARS} https://repo.maven.apache.org/maven2/com/facebook/presto/presto-jdbc/0.209/presto-jdbc-0.209.jar && \
  # BigQueryJDBC uses difficult distribution mechanism
  wget -P /tmp https://storage.googleapis.com/simba-bq-release/jdbc/SimbaJDBCDriverforGoogleBigQuery42_1.1.6.1006.zip && \   
  unzip -d ${SPARK_JARS} /tmp/SimbaJDBCDriverforGoogleBigQuery42_1.1.6.1006.zip *.jar && \
  rm ${SPARK_JARS}/jackson-core-2.1.3.jar && \
  rm /tmp/SimbaJDBCDriverforGoogleBigQuery42_1.1.6.1006.zip && \
  # logging
  wget -P ${SPARK_JARS} https://repo.maven.apache.org/maven2/com/microsoft/azure/applicationinsights-core/1.0.9/applicationinsights-core-1.0.9.jar && \           
  wget -P ${SPARK_JARS} https://repo.maven.apache.org/maven2/com/microsoft/azure/applicationinsights-logging-log4j1_2/1.0.9/applicationinsights-logging-log4j1_2-1.0.9.jar && \
  wget -P ${SPARK_JARS} https://repo.maven.apache.org/maven2/com/github/ptv-logistics/log4jala/1.0.4/log4jala-1.0.4.jar && \       
  #geospark
  wget -P ${SPARK_JARS} https://repo.maven.apache.org/maven2/org/datasyslab/geospark/1.1.3/geospark-1.1.3.jar && \       
  wget -P ${SPARK_JARS} https://repo.maven.apache.org/maven2/org/datasyslab/geospark-sql_2.3/1.1.3/geospark-sql_2.3-1.1.3.jar && \
  # google cloud
  wget -P ${SPARK_JARS} https://repo.maven.apache.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop2-1.9.5/gcs-connector-hadoop2-1.9.5.jar && \ 
  # elasticsearch
  wget -P ${SPARK_JARS} https://repo.maven.apache.org/maven2/org/elasticsearch/elasticsearch-hadoop/6.6.1/elasticsearch-hadoop-6.6.1.jar

# copy in tutorial
COPY tutorial /opt/tutorial

RUN chmod +x /opt/tutorial/nyctaxi/download_raw_data_small.sh
RUN chmod +x /opt/tutorial/nyctaxi/download_raw_data_large.sh

# copy in log4j.properties config file
COPY log4j.properties ${SPARK_HOME}/conf/log4j.properties

# copy in etl library
COPY target/scala-2.11/arc.jar ${SPARK_HOME}/jars/arc.jar

WORKDIR $SPARK_HOME
# EOF


