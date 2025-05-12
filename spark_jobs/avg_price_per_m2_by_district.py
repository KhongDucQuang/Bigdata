# File: spark_jobs/avg_price_per_m2_by_district_by_city.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, lower, trim, when

def normalize_city_name(city_col):
    city_lower = lower(trim(city_col))
    return when(city_lower.like("%hồ chí minh%"), "HoChiMinh") \
          .when(city_lower.like("%hà nội%"), "HaNoi") \
          .otherwise(None)

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Avg Price per m2 by District (HN, HCM)") \
        .getOrCreate()

    input_path = "hdfs://hadoop-namenode:8020/user/kafka_data/*.json"
    output_base_path = "hdfs://hadoop-namenode:8020/user/batch_views_spark/avg_price_per_m2_by_city"
    local_output_path = "/app/spark_jobs/output_avg_price"

    print(f"Đọc dữ liệu từ: {input_path}")

    try:
        df = spark.read.option("multiLine", "true").json(input_path)

        df = df.withColumn("city_norm", normalize_city_name(col("thanh_pho"))) \
               .filter(col("city_norm").isin("HaNoi", "HoChiMinh")) \
               .filter(col("dien_tich").isNotNull() & (col("dien_tich") >= 5)) \
               .filter(col("gia_ban").isNotNull() & (col("gia_ban") > 0))

        df = df.withColumn("gia_m2_trieu", (col("gia_ban") * 1000) / col("dien_tich"))

        result = df.groupBy("city_norm", "quan_huyen") \
                   .agg(avg("gia_m2_trieu").alias("gia_tb_m2_trieu")) \
                   .orderBy("city_norm", col("gia_tb_m2_trieu").desc())

        print("Tính toán hoàn tất. Bắt đầu ghi file...")

        result.write \
              .partitionBy("city_norm") \
              .mode("overwrite") \
              .json(output_base_path)

        print(f"Đã ghi HDFS phân vùng tại: {output_base_path}")

        # Ghi ra local (tuỳ chọn)
        result.write \
              .partitionBy("city_norm") \
              .mode("overwrite") \
              .json(local_output_path)

        print(f"Đã ghi bản sao local tại: {local_output_path}")

    except Exception as e:
        print(f"Lỗi: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        print("SparkSession đã dừng.")
