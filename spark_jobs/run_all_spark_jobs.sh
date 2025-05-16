#!/bin/bash
echo "Bắt đầu thực thi các Spark jobs..."

# Đường dẫn tới spark-submit (đã có trong ENTRYPOINT của Dockerfile, nhưng có thể gọi lại nếu cần)
SPARK_SUBMIT_CMD="/opt/bitnami/spark/bin/spark-submit"
# Cấu hình chung cho các job (thay thế bằng cấu hình thực tế của bạn)
SPARK_MASTER_URL="spark://spark-master:7077" # Thay thế spark-master bằng tên service Spark Master của bạn

# Đường dẫn tới các file job Python bên trong container (do WORKDIR /app và COPY ./spark_jobs/)
JOB_PATH_PREFIX="/app/spark_jobs"

echo "Đang chạy job: avg_cost_city_month.py"
$SPARK_SUBMIT_CMD \
  --master $SPARK_MASTER_URL \
  --packages org.elasticsearch:elasticsearch-spark-20_2.11:8.13.4 \
  $JOB_PATH_PREFIX/avg_cost_city_month.py
echo "Hoàn thành job: avg_cost_city_month.py"
echo "----------------------------------------"

echo "Đang chạy job: avg_cost_city_month_median.py"
$SPARK_SUBMIT_CMD \
  --master $SPARK_MASTER_URL \
  --packages org.elasticsearch:elasticsearch-spark-20_2.11:8.13.4 \
  $JOB_PATH_PREFIX/avg_cost_city_month_median.py
echo "Hoàn thành job: avg_cost_city_month_median.py"
echo "----------------------------------------"

echo "Đang chạy job: avg_price_per_m2_by_district.py"
$SPARK_SUBMIT_CMD \
  --master $SPARK_MASTER_URL \
  --packages org.elasticsearch:elasticsearch-spark-20_2.11:8.13.4 \
  $JOB_PATH_PREFIX/avg_price_per_m2_by_district.py
echo "Hoàn thành job: avg_price_per_m2_by_district.py"
echo "----------------------------------------"

echo "Đang chạy job: count_by_district_by_city_to_es.py"
$SPARK_SUBMIT_CMD \
  --master $SPARK_MASTER_URL \
  --packages org.elasticsearch:elasticsearch-spark-20_2.11:8.13.4 \
  $JOB_PATH_PREFIX/count_by_district.py
echo "Hoàn thành job: count_by_district_by_city_to_es.py"
echo "----------------------------------------"

echo "Tất cả các Spark jobs đã được thực thi."