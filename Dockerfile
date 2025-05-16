# Sử dụng base image Bitnami Spark 2.4.4
# Kiểm tra Docker Hub để có tag revision mới nhất nếu cần, ví dụ: bitnami/spark:2.4.4-debian-10-rXX
FROM bitnami/spark:2.4.4

# Chuyển sang user root để có quyền cài đặt các gói
USER root

# Cập nhật danh sách gói và cài đặt Python 3.7, python3.7-dev, python3.7-distutils và các công cụ cần thiết
# --no-install-recommends để giảm kích thước image
# apt-get clean và rm -rf /var/lib/apt/lists/* để dọn dẹp sau khi cài đặt
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3.7 \
        python3.7-dev \
        python3.7-distutils \
        wget \
        ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Cài đặt pip cho python3.7 (đảm bảo pip là của python3.7)
# SỬA ĐỔI Ở ĐÂY: Sử dụng URL get-pip.py đúng cho Python 3.7
RUN wget https://bootstrap.pypa.io/pip/3.7/get-pip.py && \
    python3.7 get-pip.py && \
    rm get-pip.py

# Cập nhật symbolic links để 'python3' trỏ đến phiên bản 3.7
# Xóa symlink cũ của python3 nếu nó tồn tại và không phải là thư mục
RUN if [ -L /usr/bin/python3 ]; then rm /usr/bin/python3; fi && \
    ln -sf /usr/bin/python3.7 /usr/bin/python3

# Cập nhật symlink cho pip3 để trỏ đến pip của python3.7
# pip được cài bởi get-pip.py cho một phiên bản cụ thể thường nằm trong /usr/local/bin
# và có thể được gọi là pip hoặc pip3.7
RUN if [ -L /usr/bin/pip3 ]; then rm /usr/bin/pip3; fi && \
    ln -sf /usr/local/bin/pip /usr/bin/pip3
    # Hoặc nếu get-pip.py tạo ra /usr/local/bin/pip3.7:
    # ln -sf /usr/local/bin/pip3.7 /usr/bin/pip3

# Kiểm tra lại các phiên bản để đảm bảo
RUN echo "Kiểm tra phiên bản Python sau khi cài đặt:" && \
    python3 --version && \
    pip3 --version

# Các biến môi trường bạn đã có (giữ nguyên hoặc điều chỉnh nếu cần)
ENV HOME=/tmp
ENV HADOOP_USER_NAME=root
ENV PYTHONIOENCODING=UTF-8

ENV SPARK_DRIVER_EXTRA_JAVA_OPTIONS="-Duser.name=root"
ENV SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS="-Duser.name=root"
ENV HADOOP_CONF_DIR="/opt/bitnami/spark/conf"

# Các lệnh RUN khác từ Dockerfile gốc của bạn
RUN mkdir -p $HOME/.ivy2/local && chmod -R 777 $HOME/.ivy2
RUN echo "spark.jars.ivy ${HOME}/.ivy2" >> /opt/bitnami/spark/conf/spark-defaults.conf

WORKDIR /app

# COPY các file ứng dụng Spark và cấu hình của bạn
COPY spark_jobs/ ./spark_jobs/
COPY spark_conf/core-site.xml /opt/bitnami/spark/conf/core-site.xml
# Đảm bảo script chạy có quyền thực thi (USER root đã được set ở trên)
RUN chmod +x ./spark_jobs/run_all_spark_jobs.sh # Hoặc tên script wrapper của bạn

# ENTRYPOINT của bạn (sẽ chạy với user root do USER root được đặt ở trên)
ENTRYPOINT ["/app/spark_jobs/run_all_spark_jobs.sh"]
