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

ENV JAVA_ALPINE_VERSION 8.212.04-r0

RUN set -x \
  && apk add --no-cache \
  openjdk8="$JAVA_ALPINE_VERSION" \
  && [ "$JAVA_HOME" = "$(docker-java-home)" ]

# Versions
ARG ARC_VERSION
ENV SPARK_VERSION         2.4.3
ENV SCALA_VERSION         2.11
ENV HADOOP_VERSION        2.7
ENV SPARK_HOME            /opt/spark
ENV SPARK_JARS            /opt/spark/jars/
ENV SPARK_CHECKSUM_URL    https://archive.apache.org/dist/spark
ENV SPARK_DOWNLOAD_URL    https://www-us.apache.org/dist/spark
ENV GLIBC_VERSION         2.26-r0

# Setup basics
RUN set -ex && \
  apk upgrade --update && \
  apk add --update libstdc++ ca-certificates bash openblas curl findutils && \
  for pkg in glibc-${GLIBC_VERSION} glibc-bin-${GLIBC_VERSION} glibc-i18n-${GLIBC_VERSION}; do curl -sSL https://github.com/andyshinn/alpine-pkg-glibc/releases/download/${GLIBC_VERSION}/${pkg}.apk -o /tmp/${pkg}.apk; done && \
  apk add --allow-untrusted /tmp/*.apk && \
  rm -v /tmp/*.apk && \
  ( /usr/glibc-compat/bin/localedef --force --inputfile POSIX --charmap UTF-8 C.UTF-8 || true ) && \
  echo "export LANG=C.UTF-8" > /etc/profile.d/locale.sh && \
  /usr/glibc-compat/sbin/ldconfig /lib /usr/glibc-compat/lib

# install spark
RUN mkdir -p ${SPARK_HOME} && \
  wget -O spark.sha ${SPARK_CHECKSUM_URL}/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz.sha512 && \
  export SPARK_SHA512_SUM=$(grep -o "[A-F0-9]\{8\}" spark.sha | awk '{print}' ORS='' | tr '[:upper:]' '[:lower:]') && \
  rm -f spark.sha && \
  wget -O spark.tar.gz ${SPARK_DOWNLOAD_URL}/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
  echo "${SPARK_SHA512_SUM}  spark.tar.gz" | sha512sum -c - && \
  gunzip -c spark.tar.gz | tar -xf - -C $SPARK_HOME --strip-components=1 && \
  rm -f spark.tar.gz

# download etl library and dependencies
RUN cd /tmp && \
  wget -P /tmp https://git.io/coursier-cli && \
  chmod +x /tmp/coursier-cli && \
  /tmp/coursier-cli fetch \
  --force-version com.fasterxml.jackson.core:jackson-databind:2.6.7.1 \
  --force-version org.json4s:json4s-ast_${SCALA_VERSION}:3.5.3 \
  --force-version org.json4s:json4s-core_${SCALA_VERSION}:3.5.3 \
  --force-version org.json4s:json4s-jackson_${SCALA_VERSION}:3.5.3 \
  --force-version org.json4s:json4s-scalap_${SCALA_VERSION}:3.5.3 \
  --force-version org.json4s:json4s-scalap_${SCALA_VERSION}:3.5.3 \
  --force-version org.slf4j:slf4j-log4j12:1.7.16 \
  ai.tripl:arc_${SCALA_VERSION}:${ARC_VERSION} && \
  find /root/.cache/coursier -name "*.jar" -print0 | xargs -0 -I {} mv {} ${SPARK_JARS}

# symlink the library for easy invocation
RUN ln -s ${SPARK_JARS}/arc_${SCALA_VERSION}-${ARC_VERSION}.jar ${SPARK_JARS}/arc.jar

# copy in log4j.properties config file
COPY log4j.properties ${SPARK_HOME}/conf/log4j.properties

# copy in tutorial
COPY tutorial /opt/tutorial
RUN chmod +x /opt/tutorial/nyctaxi/download_raw_data_small.sh
RUN chmod +x /opt/tutorial/nyctaxi/download_raw_data_large.sh

WORKDIR $SPARK_HOME
# EOF
