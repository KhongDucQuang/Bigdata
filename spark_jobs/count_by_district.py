# File: spark_jobs/count_by_district_by_city.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lower, trim, when

def normalize_city_name(city_col):
    city_lower = lower(trim(city_col))
    return when(city_lower.like("%hồ chí minh%"), "HoChiMinh") \
          .when(city_lower.like("%hà nội%"), "HaNoi") \
          .otherwise(None)

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("District Count by City (HN, HCM)") \
        .getOrCreate()

    input_path = "hdfs://hadoop-namenode:8020/user/kafka_data/*.json"
    output_base_path = "hdfs://hadoop-namenode:8020/user/batch_views_spark/district_counts_by_city"

    print("Đọc dữ liệu từ: {}".format(input_path))
    print("Ghi kết quả phân vùng tới: {}".format(output_base_path))

    try:
        raw_df = spark.read.option("multiLine", "true").json(input_path) # Thêm option multiLine nếu file JSON có thể nhiều dòng
        print("Đã đọc dữ liệu raw.")

        raw_df.show(10, truncate=False)

        # --- Xử lý ---
        print("Bắt đầu chuẩn hóa, lọc, và tổng hợp...")

        # Thêm cột thành phố đã chuẩn hóa và lọc
        city_filtered_df = raw_df.withColumn("city_norm", normalize_city_name(col("thanh_pho"))) \
                                 .filter(col("city_norm").isin("HaNoi", "HoChiMinh"))

        # Chọn cột, lọc quận huyện hợp lệ, nhóm và đếm
        district_counts = city_filtered_df.select(col("city_norm"), col("quan_huyen")) \
                                         .filter(col("quan_huyen").isNotNull() & (col("quan_huyen") != "")) \
                                         .groupBy("city_norm", "quan_huyen") \
                                         .agg(count("*").alias("so_luong"))

        # Sắp xếp kết quả: theo thành phố, sau đó theo số lượng giảm dần
        sorted_counts = district_counts.orderBy(col("city_norm"), col("so_luong").desc())

        print("Xử lý và sắp xếp hoàn tất.")

        #Ghi kết quả ra HDFS (phân vùng theo thành phố) ---
        print("Đang ghi kết quả phân vùng vào HDFS {}...".format(output_base_path))
        sorted_counts.write \
            .partitionBy("city_norm") \
            .mode("overwrite") \
            .json(output_base_path)
                     # Nếu muốn ghi CSV với header:
                     # .option("header", "true") \
                     # .csv(output_base_path)

        print("Ghi kết quả HDFS thành công!")
        print("Kiểm tra các thư mục con như 'city_norm=HaNoi', 'city_norm=HoChiMinh' trong '{}'".format(output_base_path))

    except Exception as e:
        print("Đã xảy ra lỗi trong quá trình xử lý Spark: {}".format(e))
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        print("SparkSession đã dừng.")