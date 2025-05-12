# Sử dụng image base chính thức của Apache Spark (Đúng tên và tag)
FROM apache/spark:3.4.3-python3

# Đặt các biến môi trường cần thiết
ENV HOME=/tmp
ENV HADOOP_USER_NAME=root
ENV SPARK_DRIVER_EXTRA_JAVA_OPTIONS="-Duser.name=root"
ENV SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS="-Duser.name=root"

# Chỉ định thư mục cấu hình Hadoop/Spark cho image Apache Spark
ENV SPARK_HOME=/opt/spark
ENV HADOOP_CONF_DIR=${SPARK_HOME}/conf

# Tạo thư mục Ivy (Giữ lại tạm thời)
RUN mkdir -p $HOME/.ivy2/local && chmod -R 777 $HOME/.ivy2

# --- THÊM LỆNH TẠO THƯ MỤC CONF ---
# Đảm bảo thư mục conf tồn tại trước khi ghi file
RUN mkdir -p ${SPARK_HOME}/conf

# Ghi cấu hình Ivy vào spark-defaults.conf
# Ghi vào đường dẫn conf mới
RUN echo "spark.jars.ivy ${HOME}/.ivy2" >> ${SPARK_HOME}/conf/spark-defaults.conf

# Đặt thư mục làm việc
WORKDIR /app

# Copy file core-site.xml (đã copy vào context spark_streaming từ bước trước)
# vào thư mục cấu hình của Spark mới
COPY core-site.xml ${HADOOP_CONF_DIR}/core-site.xml

# Copy script streaming vào thư mục làm việc
COPY *.py ./

# Giữ lại USER root vì image Apache thường chạy root mặc định và
# trạng thái chạy thành công trước đó của bạn dùng root
USER root

# Entrypoint trỏ đến spark-submit trong đường dẫn mới
ENTRYPOINT ["/opt/spark/bin/spark-submit"]

# CMD không cần thiết vì command được truyền từ docker-compose
