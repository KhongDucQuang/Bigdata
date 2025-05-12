FROM bitnami/spark:latest

ENV HOME=/tmp
ENV HADOOP_USER_NAME=root

ENV SPARK_DRIVER_EXTRA_JAVA_OPTIONS="-Duser.name=root"
ENV SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS="-Duser.name=root"
ENV HADOOP_CONF_DIR="/opt/bitnami/spark/conf"

RUN mkdir -p $HOME/.ivy2/local && chmod -R 777 $HOME/.ivy2
RUN echo "spark.jars.ivy ${HOME}/.ivy2" >> /opt/bitnami/spark/conf/spark-defaults.conf

WORKDIR /app

COPY spark_jobs/ ./spark_jobs/
COPY spark_conf/core-site.xml /opt/bitnami/spark/conf/core-site.xml

USER root

ENTRYPOINT ["/opt/bitnami/spark/bin/spark-submit"]
