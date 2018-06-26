FROM openjdk:8u151-jdk-alpine3.7

# Spark Verison
ENV SPARK_VERSION         2.3.1
ENV HADOOP_VERSION        2.7
ENV SPARK_HOME            /opt/spark
ENV SPARK_CHECKSUM_URL    https://www.apache.org/dist/spark
ENV SPARK_DOWNLOAD_URL    https://www-us.apache.org/dist/spark
ENV GLIBC_VERSION         2.26-r0

# do spark
RUN set -ex && \
    apk upgrade --update && \
    apk add --update libstdc++ curl ca-certificates bash openblas findutils && \
    for pkg in glibc-${GLIBC_VERSION} glibc-bin-${GLIBC_VERSION} glibc-i18n-${GLIBC_VERSION}; do curl -sSL https://github.com/andyshinn/alpine-pkg-glibc/releases/download/${GLIBC_VERSION}/${pkg}.apk -o /tmp/${pkg}.apk; done && \
    apk add --allow-untrusted /tmp/*.apk && \
    rm -v /tmp/*.apk && \
    ( /usr/glibc-compat/bin/localedef --force --inputfile POSIX --charmap UTF-8 C.UTF-8 || true ) && \
    echo "export LANG=C.UTF-8" > /etc/profile.d/locale.sh && \
    /usr/glibc-compat/sbin/ldconfig /lib /usr/glibc-compat/lib && \
    mkdir -p ${SPARK_HOME} && \
    curl --show-error --location --output spark.sha \
    ${SPARK_CHECKSUM_URL}/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz.sha512 && \
    export SPARK_SHA512_SUM=$(grep -o "[A-F0-9]\{8\}" spark.sha | awk '{print}' ORS='' | tr '[:upper:]' '[:lower:]') && \
    rm -f spark.sha && \
    curl --show-error --location --output spark.tar.gz ${SPARK_DOWNLOAD_URL}/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    echo "${SPARK_SHA512_SUM}  spark.tar.gz" | sha512sum -c - && \
    gunzip -c spark.tar.gz | tar -xf - -C $SPARK_HOME --strip-components=1 && \
    rm -f spark.tar.gz

# spark extensions
RUN curl  --show-error --location --output ${SPARK_HOME}/jars/spark-xml_2.11-0.4.1.jar \    
    https://repo.maven.apache.org/maven2/com/databricks/spark-xml_2.11/0.4.1/spark-xml_2.11-0.4.1.jar && \    
    curl  --show-error --location --output ${SPARK_HOME}/jars/spark-avro_2.11-3.2.0.jar \    
    https://repo.maven.apache.org/maven2/com/databricks/spark-avro_2.11/4.0.0/spark-avro_2.11-4.0.0.jar && \   
    curl  --show-error --location --output ${SPARK_HOME}/jars/spark-redshift_2.11-3.0.0-preview1.jar \    
    https://repo.maven.apache.org/maven2/com/databricks/spark-redshift_2.11/3.0.0-preview1/spark-redshift_2.11-3.0.0-preview1.jar && \  
    # aws hadoop
    curl  --show-error --location --output ${SPARK_HOME}/jars/hadoop-aws-2.7.4.jar \
    https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/2.7.4/hadoop-aws-2.7.4.jar && \
    curl  --show-error --location --output ${SPARK_HOME}/jars/aws-java-sdk-1.7.4.jar \
    https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk/1.11.197/aws-java-sdk-1.11.197.jar && \
    # azure hadoop
    curl  --show-error --location --output ${SPARK_HOME}/jars/hadoop-azure-2.7.4.jar \    
    https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-azure/2.7.4/hadoop-azure-2.7.4.jar && \
    curl  --show-error --location --output ${SPARK_HOME}/jars/azure-storage-3.1.0.jar \    
    https://repo.maven.apache.org/maven2/com/microsoft/azure/azure-storage/3.1.0/azure-storage-3.1.0.jar && \    
    # azure eventhub
    curl  --show-error --location --output ${SPARK_HOME}/jars/azure-eventhubs-1.0.1.jar  \
    https://repo.maven.apache.org/maven2/com/microsoft/azure/azure-eventhubs/1.0.1/azure-eventhubs-1.0.1.jar && \       
    curl  --show-error --location --output ${SPARK_HOME}/jars/proton-j-0.26.0.jar  \
    https://repo.maven.apache.org/maven2/org/apache/qpid/proton-j/0.26.0/proton-j-0.26.0.jar && \   
    # databases
    curl  --show-error --location --output ${SPARK_HOME}/jars/mssql-jdbc-6.1.1.jre8.jar \
    https://github.com/Microsoft/mssql-jdbc/releases/download/v6.1.1/mssql-jdbc-6.1.1.jre8.jar && \
    curl  --show-error --location --output ${SPARK_HOME}/jars/postgresql-42.2.2.jar \    
    https://jdbc.postgresql.org/download/postgresql-42.2.2.jar && \  
    curl  --show-error --location --output ${SPARK_HOME}/jars/spark-cassandra-connector_2.11-2.0.5.jar \    
    https://repo.maven.apache.org/maven2/com/datastax/spark/spark-cassandra-connector_2.11/2.0.5/spark-cassandra-connector_2.11-2.0.5.jar && \
    curl  --show-error --location --output ${SPARK_HOME}/jars/mysql-connector-java-5.1.45.jar \    
    https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/5.1.45/mysql-connector-java-5.1.45.jar && \    
    # logging
    curl  --show-error --location --output ${SPARK_HOME}/jars/applicationinsights-core-1.0.9.jar \    
    https://repo.maven.apache.org/maven2/com/microsoft/azure/applicationinsights-core/1.0.9/applicationinsights-core-1.0.9.jar && \           
    curl  --show-error --location --output ${SPARK_HOME}/jars/applicationinsights-logging-log4j1_2-1.0.9.jar \    
    https://repo.maven.apache.org/maven2/com/microsoft/azure/applicationinsights-logging-log4j1_2/1.0.9/applicationinsights-logging-log4j1_2-1.0.9.jar && \
    curl  --show-error --location --output ${SPARK_HOME}/jars/log4jala-1.0.4.jar \
    https://repo.maven.apache.org/maven2/com/github/ptv-logistics/log4jala/1.0.4/log4jala-1.0.4.jar && \       
    #geospark
    curl  --show-error --location --output ${SPARK_HOME}/jars/geospark-1.1.1.jar \
    https://repo.maven.apache.org/maven2/org/datasyslab/geospark/1.1.1/geospark-1.1.1.jar && \       
    curl  --show-error --location --output ${SPARK_HOME}/jars/geospark-sql_2.3-1.1.1.jar \
    https://repo.maven.apache.org/maven2/org/datasyslab/geospark-sql_2.3/1.1.1/geospark-sql_2.3-1.1.1.jar && \       
    apk del curl

# copy in etl library
COPY target/scala-2.11/arc.jar /opt/spark/jars/arc.jar

# copy in tutorial
COPY tutorial /opt/tutorial
RUN chmod +x /opt/tutorial/nyctaxi/download_raw_data_small.sh
RUN chmod +x /opt/tutorial/nyctaxi/download_raw_data_large.sh

# copy in log4j.properties config file
COPY log4j.properties /opt/spark/conf/log4j.properties

WORKDIR $SPARK_HOME
# EOF


