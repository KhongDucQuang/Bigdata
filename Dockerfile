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
    python3.7 get-pip.py && \
    rm get-pip.py

RUN if [ -L /usr/bin/python3 ]; then rm /usr/bin/python3; fi && \
    ln -sf /usr/bin/python3.7 /usr/bin/python3

RUN if [ -L /usr/bin/pip3 ]; then rm /usr/bin/pip3; fi && \
    ln -sf /usr/local/bin/pip /usr/bin/pip3

RUN echo "Kiểm tra phiên bản Python sau khi cài đặt:" && \
    python3 --version && \
    pip3 --version

ENV HOME=/tmp
ENV HADOOP_USER_NAME=root
ENV PYTHONIOENCODING=UTF-8

ENV SPARK_DRIVER_EXTRA_JAVA_OPTIONS="-Duser.name=root"
ENV SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS="-Duser.name=root"
ENV HADOOP_CONF_DIR="/opt/bitnami/spark/conf"

RUN mkdir -p $HOME/.ivy2/local && chmod -R 777 $HOME/.ivy2
RUN echo "spark.jars.ivy ${HOME}/.ivy2" >> /opt/bitnami/spark/conf/spark-defaults.conf

WORKDIR /app

COPY spark_jobs/ ./spark_jobs/
COPY spark_conf/core-site.xml /opt/bitnami/spark/conf/core-site.xml

RUN chmod +x ./spark_jobs/run_all_spark_jobs.sh # Hoặc tên script wrapper của bạn

ENTRYPOINT ["/app/spark_jobs/run_all_spark_jobs.sh"]
