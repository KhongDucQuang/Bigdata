from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, lower, trim, when, to_date, date_format

def normalize_city_name(city_col):
    city_lower = lower(trim(city_col))
    return when(city_lower.like("%hồ chí minh%"), "HoChiMinh") \
          .when(city_lower.like("%hà nội%"), "HaNoi") \
          .otherwise(None)

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Avg Price per m2 per Month by City (HN, HCM)") \
        .getOrCreate()

    # Phần cấu hình Hadoop
    try:
        spark.sparkContext._jsc.hadoopConfiguration().set("hadoop.security.authentication", "simple")
        spark.sparkContext._jsc.hadoopConfiguration().set("hadoop.security.authorization", "false")
        current_auth_method = spark.sparkContext._jsc.hadoopConfiguration().get("hadoop.security.authentication")
        print(f"DEBUG: hadoop.security.authentication hiện tại là: {current_auth_method}")
    except Exception as e_config:
        print(f"Lỗi khi cố gắng đặt cấu hình Hadoop: {e_config}")
    # Kết thúc phần cấu hình

    input_path = "hdfs://hadoop-namenode:8020/user/kafka_data/*.json"
    output_base_path = "hdfs://hadoop-namenode:8020/user/batch_views_spark/avg_price_per_m2_per_month_by_city"
    local_output_path = "/app/spark_jobs/output_avg_price_m2_by_month"

    print(f"Đọc dữ liệu từ: {input_path}")

    try:
        df = spark.read.option("multiLine", "true").json(input_path)

        df = df.withColumn("city_norm", normalize_city_name(col("thanh_pho"))) \
               .filter(col("city_norm").isin("HaNoi", "HoChiMinh")) \
               .filter(col("gia_ban").isNotNull() & (col("gia_ban").cast("float") > 0)) \
               .filter(col("dien_tich").isNotNull() & (col("dien_tich").cast("float") >= 5)) \
               .filter(col("ngay_dang").isNotNull())

        #Tính toán giá trên m2 cho mỗi tin
        df = df.withColumn("gia_tren_m2", (col("gia_ban").cast("float") * 1000) / col("dien_tich").cast("float")) \
               .withColumn("month", date_format(to_date(col("ngay_dang"), "yyyy-MM-dd"), "yyyy-MM"))

        #Tính giá trung bình trên m2
        result = df.groupBy("city_norm", "month") \
                   .agg(avg("gia_tren_m2").alias("gia_tb_m2_trieu")) \
                   .orderBy("city_norm", "month")

        print("Tính toán hoàn tất. Bắt đầu ghi file...")
        print("Kết quả tính toán (Giá TB / m2):")
        result.show()

        #Ghi ra HDFS
        result.write \
              .partitionBy("city_norm") \
              .mode("overwrite") \
              .json(output_base_path)
        print(f"Đã ghi HDFS phân vùng tại: {output_base_path}")

        #Ghi ra local
        print(f"Bắt đầu ghi bản sao local tại: {local_output_path}")
        result.coalesce(1).write \
              .partitionBy("city_norm") \
              .mode("overwrite") \
              .json(local_output_path)
        print(f"Đã ghi bản sao local tại: {local_output_path}")

    except Exception as e:
        print(f"Lỗi trong quá trình xử lý Spark: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        print("SparkSession đã dừng.")