# spark_streaming/spark_streaming_job.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lower, trim, when, window, count
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType, DoubleType

# --- Hàm chuẩn hóa tên thành phố ---
def normalize_city_name(city_col):
    """Chuẩn hóa tên thành phố về HaNoi hoặc HoChiMinh."""
    city_lower = lower(trim(city_col))
    return when(city_lower.like("%hồ chí minh%"), "HoChiMinh") \
          .when(city_lower.like("%hà nội%"), "HaNoi") \
          .otherwise(None)

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
    print("Bắt đầu Spark Structured Streaming job...")


    KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "real-estate-topic")
    ES_NODES = os.getenv("ES_NODES", "elasticsearch")
    ES_PORT = os.getenv("ES_PORT", "9200")
    ES_INDEX = os.getenv("ES_INDEX", "realtime_listing_counts")
    CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "hdfs://hadoop-namenode:8020/user/spark_checkpoints/streaming_listing_counts")

    # --- Khởi tạo SparkSession với cấu hình cho Elasticsearch ---
    print("Khởi tạo SparkSession...")
    spark = SparkSession.builder \
        .appName("Realtime Listing Count (HN, HCM)") \
        .config("spark.es.nodes", ES_NODES) \
        .config("spark.es.port", ES_PORT) \
        .config("spark.es.resource", ES_INDEX) \
        .config("spark.es.nodes.wan.only", "true") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
        .getOrCreate()

    try:
        spark.sparkContext._jsc.hadoopConfiguration().set("hadoop.security.authentication", "simple")
        spark.sparkContext._jsc.hadoopConfiguration().set("hadoop.security.authorization", "false")
        print(f"DEBUG: Đã đặt hadoop.security.authentication=simple.")
    except Exception as e_config:
        print(f"WARN: Không thể đặt cấu hình Hadoop: {e_config}")

    # Giảm bớt log thừa của Spark
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
            col("timestamp").cast(TimestampType()),
            col("value").cast(StringType())
        ) \
        .filter(col("value").isNotNull()) \
        .withColumn("data", from_json(col("value"), schema)) \
        .select("timestamp", "data.*")

    # --- Xử lý dữ liệu ---
    print("Chuẩn hóa tên thành phố và lọc HN/HCM...")
    transformed_df = parsed_df \
        .withColumn("city_norm", normalize_city_name(col("thanh_pho"))) \
        .filter(col("city_norm").isin("HaNoi", "HoChiMinh"))

    # --- Áp dụng Watermark và Window ---
    # Watermark giúp xử lý dữ liệu đến trễ (ví dụ: trễ tối đa 10 phút)
    # Cửa sổ thời gian 5 phút (tumbling window)
    print("Áp dụng Watermark (10 phút) và Cửa sổ (5 phút)...")
    windowed_counts = transformed_df \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy(
            window(col("timestamp"), "5 minutes", "5 minutes"), # Cửa sổ 5 phút, không trượt (tumbling)
            col("city_norm")
        ) \
        .count()

    # --- Chuẩn bị dữ liệu cho Elasticsearch Sink ---
    # Đổi tên cột window cho thân thiện hơn
    output_df = windowed_counts.select(
        col("window.start").alias("@timestamp"), # Đặt tên @timestamp thường dùng trong ES/Kibana
        col("window.start").alias("window_start"), # Giữ lại start/end nếu cần
        col("window.end").alias("window_end"),
        col("city_norm"),
        col("count").alias("listing_count")
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