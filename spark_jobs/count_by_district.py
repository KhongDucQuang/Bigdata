# File: spark_jobs/count_by_district_by_city_to_es_separate_indices.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lower, trim, when
import traceback  # Import traceback để in chi tiết lỗi

# --- Cấu hình Elasticsearch ---
ES_NODES = "elasticsearch"  # Tên service của Elasticsearch trong Docker network, hoặc IP/hostname
ES_PORT = "9200"  # Port của Elasticsearch

# --- Định nghĩa tên Index cho từng thành phố ---
ES_INDEX_HANOI = "district_counts_hanoi"  # Index cho dữ liệu Hà Nội
ES_INDEX_HOCHIMINH = "district_counts_hochiminh"  # Index cho dữ liệu TP. Hồ Chí Minh

# Cấu hình ghi vào Elasticsearch chung (sẽ tùy chỉnh 'es.resource' cho mỗi thành phố)
# Tham khảo thêm tại: https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html
es_base_write_conf = {
    "es.nodes": ES_NODES,
    "es.port": ES_PORT,
    "es.nodes.wan.only": "false",
    "es.index.auto.create": "true",
    # "es.mapping.id": "your_document_id_field"
}


def normalize_city_name(city_col):
    """Chuẩn hóa tên thành phố về 'HaNoi' hoặc 'HoChiMinh'."""
    city_lower = lower(trim(city_col))
    return when(city_lower.like("%hồ chí minh%"), "HoChiMinh") \
        .when(city_lower.like("%hà nội%"), "HaNoi") \
        .otherwise(None)


if __name__ == "__main__":
    spark_app_name = "District Count by City to Separate ES Indices (HN, HCM)"
    print(f"Khởi tạo SparkSession: {spark_app_name}")

    spark_builder = SparkSession.builder.appName(spark_app_name)

    # Thêm cấu hình Elasticsearch chung vào SparkSession
    # Các cấu hình này sẽ được sử dụng làm cơ sở, 'es.resource' sẽ được đặt riêng khi ghi
    for key, value in es_base_write_conf.items():
        spark_builder = spark_builder.config(key, value)

    spark = spark_builder.getOrCreate()

    print("SparkSession đã được tạo.")

    try:
        spark.sparkContext._jsc.hadoopConfiguration().set("hadoop.security.authentication", "simple")
        spark.sparkContext._jsc.hadoopConfiguration().set("hadoop.security.authorization", "false")
        current_auth_method = spark.sparkContext._jsc.hadoopConfiguration().get("hadoop.security.authentication")
        print(f"DEBUG: hadoop.security.authentication hiện tại là: {current_auth_method}")
    except Exception as e_config:
        print(f"Lỗi khi cố gắng đặt cấu hình Hadoop: {e_config}")

    input_path = "hdfs://hadoop-namenode:8020/user/kafka_data/*.json"
    output_hdfs_base_path = "hdfs://hadoop-namenode:8020/user/batch_views_spark/district_counts_by_city_hdfs"
    local_output_path = "/app/spark_jobs/output_district_counts_by_city_local"

    print(f"Đọc dữ liệu từ: {input_path}")
    print(f"Ghi kết quả HDFS phân vùng tới: {output_hdfs_base_path}")
    print(
        f"Ghi kết quả Elasticsearch tới các index: '{ES_INDEX_HANOI}' và '{ES_INDEX_HOCHIMINH}' trên nodes: {ES_NODES}:{ES_PORT}")

    try:
        raw_df = spark.read.option("multiLine", "true").json(input_path)
        print("Đã đọc dữ liệu raw từ HDFS.")
        raw_df.printSchema()
        raw_df.show(5, truncate=False)

        print("Bắt đầu chuẩn hóa, lọc, và tổng hợp...")
        city_filtered_df = raw_df.withColumn("city_norm", normalize_city_name(col("thanh_pho"))) \
            .filter(col("city_norm").isin("HaNoi", "HoChiMinh")) \
            .filter(col("quan_huyen").isNotNull() & (trim(col("quan_huyen")) != ""))

        if city_filtered_df.rdd.isEmpty():
            print("Không có dữ liệu sau khi lọc thành phố. Dừng xử lý.")
        else:
            print("Dữ liệu sau khi lọc thành phố:")
            city_filtered_df.show(5, truncate=False)

            district_counts = city_filtered_df.groupBy("city_norm", "quan_huyen") \
                .agg(count("*").alias("so_luong_tin"))

            sorted_counts = district_counts.orderBy(col("city_norm"), col("so_luong_tin").desc())

            print("Xử lý và sắp xếp hoàn tất.")
            print("Kết quả tổng hợp (Số lượng tin theo Quận/Huyện và Thành phố):")
            sorted_counts.show(50, truncate=False)

            if not sorted_counts.rdd.isEmpty():
                # --- Ghi kết quả ra HDFS (phân vùng theo thành phố) ---
                print(f"Đang ghi kết quả phân vùng vào HDFS {output_hdfs_base_path}...")
                sorted_counts.write \
                    .partitionBy("city_norm") \
                    .mode("overwrite") \
                    .json(output_hdfs_base_path)
                print(f"Ghi kết quả HDFS thành công! Kiểm tra các thư mục con trong '{output_hdfs_base_path}'")

                # --- Lọc và Ghi dữ liệu cho Hà Nội vào Elasticsearch ---
                hanoi_counts_df = sorted_counts.filter(col("city_norm") == "HaNoi")
                if not hanoi_counts_df.rdd.isEmpty():
                    print(f"Đang ghi dữ liệu Hà Nội vào Elasticsearch index '{ES_INDEX_HANOI}'...")
                    hanoi_counts_df.write \
                        .format("org.elasticsearch.spark.sql") \
                        .option("es.resource", ES_INDEX_HANOI) \
                        .mode("overwrite") \
                        .save()  # Không cần truyền resource vào save() nếu đã set trong option
                    print(f"Ghi dữ liệu Hà Nội vào Elasticsearch index '{ES_INDEX_HANOI}' thành công!")
                else:
                    print("Không có dữ liệu tổng hợp cho Hà Nội để ghi vào Elasticsearch.")

                # --- Lọc và Ghi dữ liệu cho TP. Hồ Chí Minh vào Elasticsearch ---
                hochiminh_counts_df = sorted_counts.filter(col("city_norm") == "HoChiMinh")
                if not hochiminh_counts_df.rdd.isEmpty():
                    print(f"Đang ghi dữ liệu TP. Hồ Chí Minh vào Elasticsearch index '{ES_INDEX_HOCHIMINH}'...")
                    hochiminh_counts_df.write \
                        .format("org.elasticsearch.spark.sql") \
                        .option("es.resource", ES_INDEX_HOCHIMINH) \
                        .mode("overwrite") \
                        .save()
                    print(f"Ghi dữ liệu TP. Hồ Chí Minh vào Elasticsearch index '{ES_INDEX_HOCHIMINH}' thành công!")
                else:
                    print("Không có dữ liệu tổng hợp cho TP. Hồ Chí Minh để ghi vào Elasticsearch.")

                # --- Ghi ra local (nếu cần để debug) ---
                print(f"Bắt đầu ghi bản sao local tại: {local_output_path}")
                sorted_counts.coalesce(1).write \
                    .partitionBy("city_norm") \
                    .mode("overwrite") \
                    .json(local_output_path)
                print(f"Đã ghi bản sao local tại: {local_output_path}")
            else:
                print("Không có dữ liệu tổng hợp để ghi.")

    except Exception as e:
        print(f"Đã xảy ra lỗi trong quá trình xử lý Spark: {e}")
        traceback.print_exc()
    finally:
        if 'spark' in locals() and spark:
            spark.stop()
            print("SparkSession đã dừng.")
