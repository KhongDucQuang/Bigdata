# File: spark_jobs/avg_cost_city_month_median_to_es.py
# Nhớ import thêm percentile_approx
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, lower, trim, when, to_date, date_format, percentile_approx
import traceback  # Import traceback để in chi tiết lỗi

# --- Cấu hình Elasticsearch ---
ES_NODES = "elasticsearch"  # Tên service của Elasticsearch trong Docker network
ES_PORT = "9200"  # Port của Elasticsearch

# --- Định nghĩa tên Index cho từng thành phố cho job này ---
ES_INDEX_MEDIAN_PRICE_MONTH_HANOI = "median_price_m2_month_hanoi"
ES_INDEX_MEDIAN_PRICE_MONTH_HOCHIMINH = "median_price_m2_month_hochiminh"

# Cấu hình ghi vào Elasticsearch chung
es_base_write_conf = {
    "es.nodes": ES_NODES,
    "es.port": ES_PORT,
    "es.nodes.wan.only": "false",
    "es.index.auto.create": "true",  # Tự động tạo index nếu chưa tồn tại
}


def normalize_city_name(city_col):
    """Chuẩn hóa tên thành phố về 'HaNoi' hoặc 'HoChiMinh'."""
    city_lower = lower(trim(city_col))
    return when(city_lower.like("%hồ chí minh%"), "HoChiMinh") \
        .when(city_lower.like("%hà nội%"), "HaNoi") \
        .otherwise(None)


if __name__ == "__main__":
    spark_app_name = "Median and Avg Price per m2 per Month by City to ES (HN, HCM)"
    print(f"Khởi tạo SparkSession: {spark_app_name}")

    spark_builder = SparkSession.builder.appName(spark_app_name)

    # Thêm cấu hình Elasticsearch chung vào SparkSession
    for key, value in es_base_write_conf.items():
        spark_builder = spark_builder.config(key, value)

    spark = spark_builder.getOrCreate()
    print("SparkSession đã được tạo.")

    # Phần cấu hình Hadoop
    try:
        spark.sparkContext._jsc.hadoopConfiguration().set("hadoop.security.authentication", "simple")
        spark.sparkContext._jsc.hadoopConfiguration().set("hadoop.security.authorization", "false")
        current_auth_method = spark.sparkContext._jsc.hadoopConfiguration().get("hadoop.security.authentication")
        print(f"DEBUG: hadoop.security.authentication hiện tại là: {current_auth_method}")
    except Exception as e_config:
        print(f"Lỗi khi cố gắng đặt cấu hình Hadoop: {e_config}")

    input_path = "hdfs://hadoop-namenode:8020/user/kafka_data/*.json"
    # Đường dẫn output HDFS (giữ lại nếu bạn vẫn muốn ghi ra HDFS)
    output_hdfs_base_path = "hdfs://hadoop-namenode:8020/user/batch_views_spark/median_avg_price_per_m2_per_month_by_city_hdfs"
    # Đường dẫn output local (giữ lại nếu bạn vẫn muốn ghi ra local)
    local_output_path = "/app/spark_jobs/output_median_avg_price_m2_by_month_local"

    print(f"Đọc dữ liệu từ: {input_path}")
    print(f"Ghi kết quả HDFS phân vùng tới: {output_hdfs_base_path}")
    print(
        f"Ghi kết quả Elasticsearch tới các index: '{ES_INDEX_MEDIAN_PRICE_MONTH_HANOI}' và '{ES_INDEX_MEDIAN_PRICE_MONTH_HOCHIMINH}'")

    try:
        raw_df = spark.read.option("multiLine", "true").json(input_path)
        print("Đã đọc dữ liệu raw từ HDFS.")
        raw_df.printSchema()
        raw_df.show(5, truncate=False)

        print("Bắt đầu chuẩn hóa, lọc, và tính toán giá/m2...")

        # Chuẩn hóa thành phố và áp dụng các bộ lọc
        # (Giữ lại các bộ lọc bạn đã có cho gia_ban và dien_tich nếu chúng phù hợp cho cả tính trung vị)
        processed_df = raw_df.withColumn("city_norm", normalize_city_name(col("thanh_pho"))) \
            .filter(col("city_norm").isin("HaNoi", "HoChiMinh")) \
            .filter(col("gia_ban").isNotNull() & (col("gia_ban").cast("float") > 0)) \
            .filter(col("dien_tich").isNotNull() & (col("dien_tich").cast("float") >= 5)) \
            .filter(col("ngay_dang").isNotNull())

        if processed_df.rdd.isEmpty():
            print("Không có dữ liệu sau khi lọc ban đầu. Dừng xử lý.")
        else:
            print("Dữ liệu sau khi lọc ban đầu:")
            processed_df.show(5, truncate=False)

            # --- Tính toán giá trên m2 cho mỗi tin (đơn vị triệu VNĐ/m2, giả định gia_ban là TỶ VNĐ)
            df_with_price_m2_month = processed_df.withColumn("gia_tren_m2", (col("gia_ban").cast("float") * 1000) / col(
                "dien_tich").cast("float")) \
                .withColumn("month", date_format(to_date(col("ngay_dang"), "yyyy-MM-dd"), "yyyy-MM"))

            print("Dữ liệu sau khi tính giá/m2 và tháng:")
            df_with_price_m2_month.select("city_norm", "month", "gia_ban", "dien_tich", "gia_tren_m2").show(5,
                                                                                                            truncate=False)

            # --- Tính giá TRUNG VỊ (Median) và TRUNG BÌNH (Average) trên m2
            result_df = df_with_price_m2_month.groupBy("city_norm", "month") \
                .agg(
                percentile_approx("gia_tren_m2", 0.5).alias("gia_trung_vi_m2_trieu"),
                avg("gia_tren_m2").alias("gia_tb_m2_trieu")  # Giữ lại trung bình để so sánh
            ) \
                .orderBy("city_norm", "month")

            print("Tính toán giá trung vị và trung bình/m2 theo tháng hoàn tất.")
            print("Kết quả tổng hợp (Giá Trung vị/m2 và Giá TB/m2 theo Tháng và Thành phố):")
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
                hanoi_median_price_month_df = result_df.filter(col("city_norm") == "HaNoi")
                if not hanoi_median_price_month_df.rdd.isEmpty():
                    print(
                        f"Đang ghi dữ liệu giá trung vị/TB theo tháng (Hà Nội) vào Elasticsearch index '{ES_INDEX_MEDIAN_PRICE_MONTH_HANOI}'...")
                    hanoi_median_price_month_df.write \
                        .format("org.elasticsearch.spark.sql") \
                        .option("es.resource", ES_INDEX_MEDIAN_PRICE_MONTH_HANOI) \
                        .mode("overwrite") \
                        .save()
                    print(
                        f"Ghi dữ liệu giá trung vị/TB theo tháng (Hà Nội) vào Elasticsearch index '{ES_INDEX_MEDIAN_PRICE_MONTH_HANOI}' thành công!")
                else:
                    print("Không có dữ liệu giá trung vị/TB theo tháng cho Hà Nội để ghi vào Elasticsearch.")

                # --- Lọc và Ghi dữ liệu cho TP. Hồ Chí Minh vào Elasticsearch ---
                hochiminh_median_price_month_df = result_df.filter(col("city_norm") == "HoChiMinh")
                if not hochiminh_median_price_month_df.rdd.isEmpty():
                    print(
                        f"Đang ghi dữ liệu giá trung vị/TB theo tháng (TP. Hồ Chí Minh) vào Elasticsearch index '{ES_INDEX_MEDIAN_PRICE_MONTH_HOCHIMINH}'...")
                    hochiminh_median_price_month_df.write \
                        .format("org.elasticsearch.spark.sql") \
                        .option("es.resource", ES_INDEX_MEDIAN_PRICE_MONTH_HOCHIMINH) \
                        .mode("overwrite") \
                        .save()
                    print(
                        f"Ghi dữ liệu giá trung vị/TB theo tháng (TP. Hồ Chí Minh) vào Elasticsearch index '{ES_INDEX_MEDIAN_PRICE_MONTH_HOCHIMINH}' thành công!")
                else:
                    print("Không có dữ liệu giá trung vị/TB theo tháng cho TP. Hồ Chí Minh để ghi vào Elasticsearch.")

                # --- Ghi ra local (nếu cần để debug) ---
                print(f"Bắt đầu ghi bản sao local tại: {local_output_path}")
                result_df.coalesce(1).write \
                    .partitionBy("city_norm") \
                    .mode("overwrite") \
                    .json(local_output_path)
                print(f"Đã ghi bản sao local tại: {local_output_path}")
            else:
                print("Không có dữ liệu tổng hợp giá trung vị/TB theo tháng để ghi.")

    except Exception as e:
        print(f"Đã xảy ra lỗi trong quá trình xử lý Spark: {e}")
        traceback.print_exc()
    finally:
        if 'spark' in locals() and spark:
            spark.stop()
            print("SparkSession đã dừng.")
