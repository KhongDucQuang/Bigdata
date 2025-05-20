# File: spark_jobs/avg_price_per_m2_by_district_to_es.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, lower, trim, when
import traceback  # Import traceback để in chi tiết lỗi

# --- Cấu hình Elasticsearch ---
ES_NODES = "elasticsearch"  # Tên service của Elasticsearch trong Docker network
ES_PORT = "9200"  # Port của Elasticsearch

# --- Định nghĩa tên Index cho từng thành phố cho job này ---
ES_INDEX_AVG_PRICE_HANOI = "avg_price_m2_district_hanoi"
ES_INDEX_AVG_PRICE_HOCHIMINH = "avg_price_m2_district_hochiminh"

# Cấu hình ghi vào Elasticsearch chung
es_base_write_conf = {
    "es.nodes": ES_NODES,
    "es.port": ES_PORT,
    "es.nodes.wan.only": "false",
    "es.index.auto.create": "true",
}


def normalize_city_name(city_col):
    """Chuẩn hóa tên thành phố về 'HaNoi' hoặc 'HoChiMinh'."""
    city_lower = lower(trim(city_col))
    return when(city_lower.like("%hồ chí minh%"), "HoChiMinh") \
        .when(city_lower.like("%hà nội%"), "HaNoi") \
        .otherwise(None)


if __name__ == "__main__":
    spark_app_name = "Avg Price per m2 by District to ES (HN, HCM)"
    print(f"Khởi tạo SparkSession: {spark_app_name}")

    spark_builder = SparkSession.builder.appName(spark_app_name)

    # Thêm cấu hình Elasticsearch chung vào SparkSession
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
    # Đường dẫn output HDFS (giữ lại nếu bạn vẫn muốn ghi ra HDFS)
    output_hdfs_base_path = "hdfs://hadoop-namenode:8020/user/batch_views_spark/avg_price_per_m2_by_district_hdfs"
    # Đường dẫn output local (giữ lại nếu bạn vẫn muốn ghi ra local)
    local_output_path = "/app/spark_jobs/output_avg_price_m2_district_local"

    print(f"Đọc dữ liệu từ: {input_path}")
    print(f"Ghi kết quả HDFS phân vùng tới: {output_hdfs_base_path}")
    print(f"Ghi kết quả Elasticsearch tới các index: '{ES_INDEX_AVG_PRICE_HANOI}' và '{ES_INDEX_AVG_PRICE_HOCHIMINH}'")

    try:
        raw_df = spark.read.option("multiLine", "true").json(input_path)
        print("Đã đọc dữ liệu raw từ HDFS.")
        raw_df.printSchema()
        raw_df.show(5, truncate=False)

        print("Bắt đầu chuẩn hóa, lọc, và tính toán giá/m2...")

        # Chuẩn hóa thành phố và lọc
        processed_df = raw_df.withColumn("city_norm", normalize_city_name(col("thanh_pho"))) \
            .filter(col("city_norm").isin("HaNoi", "HoChiMinh")) \
            .filter(col("dien_tich").isNotNull() & (col("dien_tich").cast("float") >= 10)) \
            .filter(col("gia_ban").isNotNull() & (col("gia_ban").cast("float") > 0) & (col("gia_ban").cast("float") < 1000)) \
            .filter(col("quan_huyen").isNotNull() & (trim(col("quan_huyen")) != ""))

        if processed_df.rdd.isEmpty():
            print("Không có dữ liệu sau khi lọc ban đầu. Dừng xử lý.")
        else:
            print("Dữ liệu sau khi lọc ban đầu:")
            processed_df.show(5, truncate=False)

            # Tính giá trên m2 (đơn vị triệu VNĐ/m2)
            # Giả định 'gia_ban' là TỶ VNĐ, nên nhân 1000 để ra TRIỆU VNĐ
            df_with_price_m2 = processed_df.withColumn("gia_m2_trieu",
                                                       (col("gia_ban").cast("float") * 1000) / col("dien_tich").cast(
                                                           "float"))

            print("Dữ liệu sau khi tính giá/m2:")
            df_with_price_m2.select("city_norm", "quan_huyen", "gia_ban", "dien_tich", "gia_m2_trieu").show(5,
                                                                                                            truncate=False)

            # Tính giá trung bình trên m2 theo quận/huyện và thành phố
            result_df = df_with_price_m2.groupBy("city_norm", "quan_huyen") \
                .agg(avg("gia_m2_trieu").alias("gia_tb_m2_trieu")) \
                .orderBy("city_norm", col("gia_tb_m2_trieu").desc())

            print("Tính toán giá trung bình/m2 hoàn tất.")
            print("Kết quả tổng hợp (Giá TB/m2 theo Quận/Huyện và Thành phố):")
            result_df.show(50, truncate=False)

            if not result_df.rdd.isEmpty():
                # --- Ghi kết quả ra HDFS (phân vùng theo thành phố) ---
                print(f"Đang ghi kết quả phân vùng vào HDFS {output_hdfs_base_path}...")
                result_df.write \
                    .partitionBy("city_norm") \
                    .mode("overwrite") \
                    .json(output_hdfs_base_path)
                print(f"Ghi kết quả HDFS thành công! Kiểm tra các thư mục con trong '{output_hdfs_base_path}'")

                # --- Lọc và Ghi dữ liệu cho Hà Nội vào Elasticsearch ---
                hanoi_avg_price_df = result_df.filter(col("city_norm") == "HaNoi")
                if not hanoi_avg_price_df.rdd.isEmpty():
                    print(f"Đang ghi dữ liệu giá TB/m2 Hà Nội vào Elasticsearch index '{ES_INDEX_AVG_PRICE_HANOI}'...")
                    hanoi_avg_price_df.write \
                        .format("org.elasticsearch.spark.sql") \
                        .option("es.resource", ES_INDEX_AVG_PRICE_HANOI) \
                        .mode("overwrite") \
                        .save()
                    print(
                        f"Ghi dữ liệu giá TB/m2 Hà Nội vào Elasticsearch index '{ES_INDEX_AVG_PRICE_HANOI}' thành công!")
                else:
                    print("Không có dữ liệu giá TB/m2 cho Hà Nội để ghi vào Elasticsearch.")

                # --- Lọc và Ghi dữ liệu cho TP. Hồ Chí Minh vào Elasticsearch ---
                hochiminh_avg_price_df = result_df.filter(col("city_norm") == "HoChiMinh")
                if not hochiminh_avg_price_df.rdd.isEmpty():
                    print(
                        f"Đang ghi dữ liệu giá TB/m2 TP. Hồ Chí Minh vào Elasticsearch index '{ES_INDEX_AVG_PRICE_HOCHIMINH}'...")
                    hochiminh_avg_price_df.write \
                        .format("org.elasticsearch.spark.sql") \
                        .option("es.resource", ES_INDEX_AVG_PRICE_HOCHIMINH) \
                        .mode("overwrite") \
                        .save()
                    print(
                        f"Ghi dữ liệu giá TB/m2 TP. Hồ Chí Minh vào Elasticsearch index '{ES_INDEX_AVG_PRICE_HOCHIMINH}' thành công!")
                else:
                    print("Không có dữ liệu giá TB/m2 cho TP. Hồ Chí Minh để ghi vào Elasticsearch.")

                # --- Ghi ra local (nếu cần để debug) ---
                print(f"Bắt đầu ghi bản sao local tại: {local_output_path}")
                result_df.coalesce(1).write \
                    .partitionBy("city_norm") \
                    .mode("overwrite") \
                    .json(local_output_path)
                print(f"Đã ghi bản sao local tại: {local_output_path}")
            else:
                print("Không có dữ liệu tổng hợp giá TB/m2 để ghi.")

    except Exception as e:
        print(f"Đã xảy ra lỗi trong quá trình xử lý Spark: {e}")
        traceback.print_exc()
    finally:
        if 'spark' in locals() and spark:
            spark.stop()
            print("SparkSession đã dừng.")
