# spark_streaming/streaming_price_anomaly_alert.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lower, trim, when, lit, abs as spark_abs
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType


# --- Hàm chuẩn hóa tên thành phố (Giữ nguyên) ---
def normalize_city_name(city_col):
    """Chuẩn hóa tên thành phố về HaNoi hoặc HoChiMinh."""
    city_lower = lower(trim(city_col))
    return when(city_lower.like("%hồ chí minh%"), "HoChiMinh") \
        .when(city_lower.like("%hà nội%"), "HaNoi") \
        .otherwise(None)


# --- Định nghĩa Schema cho dữ liệu JSON từ Kafka (Giữ nguyên) ---
kafka_message_schema = StructType([
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
    StructField("gia_ban", DoubleType(), True)
])

# --- Định nghĩa Schema cho NỘI DUNG FILE trong dữ liệu Batch View ---
batch_file_content_schema = StructType([
    StructField("quan_huyen", StringType(), True),
    StructField("gia_tb_m2_trieu", DoubleType(), True)  # Khớp với file JSON mẫu
])

if __name__ == "__main__":
    print("Bắt đầu Spark Structured Streaming job: Phát hiện giá BĐS bất thường (TEST MODE)...")

    KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "real-estate-topic")
    ES_NODES = os.getenv("ES_NODES", "elasticsearch")
    ES_PORT = os.getenv("ES_PORT", "9200")
    ES_INDEX_ANOMALY = os.getenv("ES_INDEX_ANOMALY", "realtime_price_anomalies_test")  # Dùng index test
    CHECKPOINT_LOCATION_ANOMALY = os.getenv("CHECKPOINT_LOCATION_ANOMALY_TEST",
                                            "hdfs://hadoop-namenode:8020/user/spark_checkpoints/streaming_price_anomaly")  # Dùng checkpoint test

    # --- THAY ĐỔI ĐƯỜNG DẪN BATCH VIEW ĐỂ TRỎ ĐẾN DỮ LIỆU TEST ---
    BATCH_VIEW_HDFS_PATH = "hdfs://hadoop-namenode:8020/user/batch_views_spark/avg_price_per_m2_by_city"

    ANOMALY_THRESHOLD_PERCENT = float(os.getenv("ANOMALY_THRESHOLD_PERCENT", "0.30"))

    print("Khởi tạo SparkSession...")
    spark = SparkSession.builder \
        .appName("Realtime Price Anomaly Detection (Test Mode)") \
        .config("spark.es.nodes", ES_NODES) \
        .config("spark.es.port", ES_PORT) \
        .config("spark.es.resource", ES_INDEX_ANOMALY) \
        .config("spark.es.nodes.wan.only", "true") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION_ANOMALY) \
        .getOrCreate()

    try:
        spark.sparkContext._jsc.hadoopConfiguration().set("hadoop.security.authentication", "simple")
        spark.sparkContext._jsc.hadoopConfiguration().set("hadoop.security.authorization", "false")
        print(f"DEBUG: Đã đặt hadoop.security.authentication=simple.")
    except Exception as e_config:
        print(f"WARN: Không thể đặt cấu hình Hadoop: {e_config}")

    spark.sparkContext.setLogLevel("WARN")
    print("SparkSession đã sẵn sàng.")

    print(f"Đọc dữ liệu Batch View từ HDFS (TEST DATA): {BATCH_VIEW_HDFS_PATH}")
    try:
        batch_view_df = spark.read.schema(batch_file_content_schema).json(BATCH_VIEW_HDFS_PATH)

        print("Schema của Batch View sau khi đọc (bao gồm cột phân vùng):")
        batch_view_df.printSchema()

        # --- TẠM THỜI COMMENT CÁC DÒNG SAU ĐỂ TEST ---
        # batch_view_df = batch_view_df.withColumnRenamed("gia_tb_m2_trieu", "gia_tham_chieu_m2")
        # batch_view_df.persist()
        # batch_view_df.show(5, truncate=False) # Có thể giữ lại để xem dữ liệu test
        # print(f"Đã đọc thành công {batch_view_df.count()} dòng từ Batch View.")
        print("ĐÃ BỎ QUA persist() và count() cho batch_view_df để test.")
        batch_view_df.show(5, truncate=False)  # Vẫn show để xem dữ liệu test được đọc chưa

    except Exception as e_batch_read:
        print(f"LỖI NGHIÊM TRỌNG: Không thể đọc dữ liệu Batch View từ {BATCH_VIEW_HDFS_PATH}. Lỗi: {e_batch_read}")
        spark.stop()
        exit(1)

    print(f"Đọc dữ liệu từ Kafka - Broker: {KAFKA_BROKER}, Topic: {KAFKA_TOPIC}")
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    print("Parse dữ liệu JSON từ cột 'value' của Kafka...")
    parsed_stream_df = kafka_df \
        .select(
        col("timestamp").cast(TimestampType()).alias("kafka_timestamp"),
        col("value").cast(StringType())
    ) \
        .filter(col("value").isNotNull()) \
        .withColumn("data", from_json(col("value"), kafka_message_schema)) \
        .select("kafka_timestamp", "data.*")

    print("Chuẩn hóa và tính giá/m2 cho dữ liệu streaming...")
    stream_df_processed = parsed_stream_df \
        .withColumn("city_norm", normalize_city_name(col("thanh_pho"))) \
        .filter(col("city_norm").isin("HaNoi", "HoChiMinh")) \
        .filter(col("quan_huyen").isNotNull()) \
        .filter(col("gia_ban").isNotNull() & (col("gia_ban").cast("float") > 0)) \
        .filter(col("dien_tich").isNotNull() & (col("dien_tich").cast("float") >= 5)) \
        .withColumn("gia_tren_m2_moi", (col("gia_ban").cast("float") * 1000) / col("dien_tich").cast("float"))

    print("Thực hiện Stream-Static Join với Batch View...")
    # Sử dụng tên cột gốc từ batch_file_content_schema là 'gia_tb_m2_trieu'
    # và đổi tên nó thành 'gia_tham_chieu_m2' sau khi join
    joined_df = stream_df_processed.join(
        batch_view_df,
        (stream_df_processed.city_norm == batch_view_df.city_norm) & \
        (stream_df_processed.quan_huyen == batch_view_df.quan_huyen),
        "inner"
    ).select(
        stream_df_processed["*"],
        col("gia_tb_m2_trieu").alias("gia_tham_chieu_m2")  # Đổi tên ở đây
    )

    print(f"Phát hiện bất thường với ngưỡng: {ANOMALY_THRESHOLD_PERCENT * 100}%")
    anomalies_df = joined_df.withColumn(
        "phan_tram_chenh_lech",
        (col("gia_tren_m2_moi") - col("gia_tham_chieu_m2")) / col("gia_tham_chieu_m2")
    ).filter(
        (spark_abs(col("phan_tram_chenh_lech")) > ANOMALY_THRESHOLD_PERCENT) & \
        (col("gia_tham_chieu_m2") > 0)
    )

    final_anomalies_df = anomalies_df.select(
        col("kafka_timestamp").alias("@timestamp"),
        col("ngay_dang"),
        col("duong_pho"),
        col("phuong_xa"),
        col("quan_huyen"),
        col("city_norm").alias("thanh_pho_norm"),
        col("dien_tich"),
        col("gia_ban"),
        col("gia_tren_m2_moi"),
        col("gia_tham_chieu_m2"),
        col("phan_tram_chenh_lech")
    )

    print(f"Bắt đầu ghi các tin đăng bất thường vào Elasticsearch index: {ES_INDEX_ANOMALY}")
    query = final_anomalies_df.writeStream \
        .format("org.elasticsearch.spark.sql") \
        .option("checkpointLocation", CHECKPOINT_LOCATION_ANOMALY) \
        .outputMode("append") \
        .option("es.resource", ES_INDEX_ANOMALY) \
        .option("es.nodes", ES_NODES) \
        .option("es.port", ES_PORT) \
        .option("es.nodes.wan.only", "true") \
        .start()

    print(f"Streaming query '{query.name}' (id: {query.id}) đã bắt đầu.")
    print(f"Kiểm tra Kibana index: {ES_INDEX_ANOMALY} để xem các tin đăng bất thường.")
    print(f"Checkpoint tại: {CHECKPOINT_LOCATION_ANOMALY}")
    print("Nhấn Ctrl+C để dừng job.")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Đã nhận tín hiệu dừng (Ctrl+C)...")
    finally:
        print("Đang dừng SparkSession...")
        spark.stop()
        print("SparkSession đã dừng.")
