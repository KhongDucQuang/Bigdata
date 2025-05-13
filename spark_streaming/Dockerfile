FROM apache/spark:3.4.3-python3

ENV HOME=/tmp
ENV HADOOP_USER_NAME=root
ENV SPARK_DRIVER_EXTRA_JAVA_OPTIONS="-Duser.name=root"
ENV SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS="-Duser.name=root"

ENV SPARK_HOME=/opt/spark
ENV HADOOP_CONF_DIR=${SPARK_HOME}/conf

RUN mkdir -p $HOME/.ivy2/local && chmod -R 777 $HOME/.ivy2
RUN mkdir -p ${SPARK_HOME}/conf
RUN echo "spark.jars.ivy ${HOME}/.ivy2" >> ${SPARK_HOME}/conf/spark-defaults.conf

WORKDIR /app

COPY core-site.xml ${HADOOP_CONF_DIR}/core-site.xml
COPY *.py ./

USER root
ENTRYPOINT ["/opt/spark/bin/spark-submit"]

