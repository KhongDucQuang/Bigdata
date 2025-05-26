FROM bitnami/spark:2.4.4

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3.7 \
        python3.7-dev \
        python3.7-distutils \
        wget \
        ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN wget https://bootstrap.pypa.io/pip/3.7/get-pip.py && \
    /usr/bin/python3.7 get-pip.py && \
    rm get-pip.py

RUN /usr/bin/python3.7 -m pip install numpy pandas matplotlib

# Chọn python3.7 làm mặc định
RUN ln -sf /usr/bin/python3.7 /usr/bin/python3 && \
    ln -sf /usr/local/bin/pip /usr/bin/pip3

ENV PYTHONIOENCODING=UTF-8
ENV PYSPARK_PYTHON=/usr/bin/python3.7
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3.7

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

RUN chmod +x ./spark_jobs/run_all_spark_jobs.sh

ENTRYPOINT ["/app/spark_jobs/run_all_spark_jobs.sh"]
