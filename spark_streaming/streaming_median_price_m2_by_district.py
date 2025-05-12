# spark_streaming/streaming_median_price_m2_by_district.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lower, trim, when, window, percentile_approx, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# --- Hàm chuẩn hóa tên thành phố (Giữ nguyên) ---
def normalize_city_name(city_col):
    """Chuẩn hóa tên thành phố về HaNoi hoặc HoChiMinh."""
    city_lower = lower(trim(city_col))
    return when(city_lower.like("%hồ chí minh%"), "HoChiMinh") \
          .when(city_lower.like("%hà nội%"), "HaNoi") \
          .otherwise(None)

# --- Định nghĩa Schema cho dữ liệu JSON từ Kafka (Giữ nguyên) ---
schema = StructType([
    StructField("ngay_dang", StringType(), True),
    StructField("duong_pho", StringType(), True),
    StructField("phuong_xa", StringType(), True),
    StructField("quan_huyen", StringType(), True),
    StructField("thanh_pho", StringType(), True),
    StructField("dien_tich", DoubleType(), True),
    StructField("chieu_ngang", DoubleType(), True),
    StructField("chieu_dai", DoubleType(), True),
    StructField("duong_truoc_nha", DoubleType(), True),
    StructField("so_tang", IntegerType(), True),
    StructField("so_phong_ngu", IntegerType(), True),
    StructField("cho_de_xe", StringType(), True),
    StructField("gia_ban", DoubleType(), True) # Giả định đơn vị là Tỷ VND
])

if __name__ == "__main__":
    print("Bắt đầu Spark Structured Streaming job: Tính giá trung vị/m2 theo Quận/Huyện...")

    # --- Lấy các tham số từ biến môi trường hoặc đặt giá trị mặc định ---
    KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "real-estate-topic")
    ES_NODES = os.getenv("ES_NODES", "elasticsearch")
    ES_PORT = os.getenv("ES_PORT", "9200")
    # Tên Index Elasticsearch MỚI cho kết quả này
    ES_INDEX = os.getenv("ES_INDEX_MEDIAN_DISTRICT", "realtime_district_median_price_m2")
    # Checkpoint Location MỚI và DUY NHẤT cho job này
    CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION_MEDIAN_DISTRICT", "hdfs://hadoop-namenode:8020/user/spark_checkpoints/streaming_median_price_district")
    WINDOW_DURATION = os.getenv("WINDOW_DURATION", "5 minutes")
    WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "5 minutes")

    # --- Khởi tạo SparkSession với cấu hình cho Elasticsearch ---
    print("Khởi tạo SparkSession...")
    spark = SparkSession.builder \
        .appName("Realtime Median Price per m2 by District") \
        .config("spark.es.nodes", ES_NODES) \
        .config("spark.es.port", ES_PORT) \
        .config("spark.es.resource", ES_INDEX) \
        .config("spark.es.nodes.wan.only", "true") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
        .getOrCreate()

    # --- (Tùy chọn) Ghi đè cấu hình Hadoop nếu cần ---
    try:
        spark.sparkContext._jsc.hadoopConfiguration().set("hadoop.security.authentication", "simple")
        spark.sparkContext._jsc.hadoopConfiguration().set("hadoop.security.authorization", "false")
        print(f"DEBUG: Đã đặt hadoop.security.authentication=simple.")
    except Exception as e_config:
        print(f"WARN: Không thể đặt cấu hình Hadoop: {e_config}")

    spark.sparkContext.setLogLevel("WARN")
    print("SparkSession đã sẵn sàng.")

    # --- Đọc dữ liệu streaming từ Kafka ---
    print(f"Đọc dữ liệu từ Kafka - Broker: {KAFKA_BROKER}, Topic: {KAFKA_TOPIC}")
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # --- Parse JSON và Trích xuất dữ liệu ---
    print("Parse dữ liệu JSON từ cột 'value' của Kafka...")
    parsed_df = kafka_df \
        .select(
            col("timestamp").cast(TimestampType()), # Lấy timestamp từ Kafka message
            col("value").cast(StringType())
        ) \
        .filter(col("value").isNotNull()) \
        .withColumn("data", from_json(col("value"), schema)) \
        .select("timestamp", "data.*")

    # --- Xử lý dữ liệu ---
    print("Chuẩn hóa tên thành phố, lọc HN/HCM, và tính giá/m2...")
    transformed_df = parsed_df \
        .withColumn("city_norm", normalize_city_name(col("thanh_pho"))) \
        .filter(col("city_norm").isin("HaNoi", "HoChiMinh")) \
        .filter(col("quan_huyen").isNotNull()) \
        .filter(col("gia_ban").isNotNull() & (col("gia_ban").cast("float") > 0)) \
        .filter(col("dien_tich").isNotNull() & (col("dien_tich").cast("float") >= 5)) \
        .withColumn("gia_tren_m2", (col("gia_ban").cast("float") * 1000) / col("dien_tich").cast("float"))

    # --- Áp dụng Watermark và Window ---
    print(f"Áp dụng Watermark ({WATERMARK_DELAY}) và Cửa sổ ({WINDOW_DURATION})...")
    windowed_median_price = transformed_df \
        .withWatermark("timestamp", WATERMARK_DELAY) \
        .groupBy(
            window(col("timestamp"), WINDOW_DURATION, WINDOW_DURATION), # Cửa sổ không trượt (tumbling)
            col("city_norm"),
            col("quan_huyen") # Nhóm thêm theo quận/huyện
        ) \
        .agg(
            percentile_approx("gia_tren_m2", 0.5).alias("gia_trung_vi_m2_moi"),
            count("*").alias("so_luong_tin_trong_cua_so")
        )

    # --- Chuẩn bị dữ liệu cho Elasticsearch Sink ---
    output_df = windowed_median_price.select(
        col("window.start").alias("@timestamp"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("city_norm"),
        col("quan_huyen"),
        col("gia_trung_vi_m2_moi"),
        col("so_luong_tin_trong_cua_so")
    )

    # --- Ghi dữ liệu streaming vào Elasticsearch ---
    print(f"Bắt đầu ghi stream vào Elasticsearch index: {ES_INDEX}")
    query = output_df.writeStream \
        .format("org.elasticsearch.spark.sql") \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .outputMode("append") \
        .option("es.resource", ES_INDEX) \
        .option("es.nodes", ES_NODES) \
        .option("es.port", ES_PORT) \
        .option("es.nodes.wan.only", "true") \
        .start()

    print(f"Streaming query '{query.name}' (id: {query.id}) đã bắt đầu.")
    print(f"Kiểm tra Kibana index: {ES_INDEX}")
    print(f"Checkpoint tại: {CHECKPOINT_LOCATION}")
    print("Nhấn Ctrl+C để dừng job.")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Đã nhận tín hiệu dừng (Ctrl+C)...")
    finally:
        print("Đang dừng SparkSession...")
        spark.stop()
        print("SparkSession đã dừng.")
