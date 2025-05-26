
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, lower, trim, when, count, regexp_replace,
    to_date, year, month, concat, stddev, min, max,
    expr,  # Giữ lại expr, xóa percentile_approx nếu chỉ dùng qua expr
    rank, desc, lag, datediff,
    window, sum, lit, corr
)
from pyspark.sql.functions import col, to_date, date_format

# Xóa percentile_approx khỏi import nếu không dùng trực tiếp nữa
# from pyspark.sql.functions import percentile_approx # Dòng này có thể xóa hoặc giữ nếu bạn dùng ở chỗ khác
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from datetime import datetime
import traceback
import os
import json


def normalize_city_name(city_col):
    """Standardize city names to HaNoi or HoChiMinh."""
    city_lower = lower(trim(city_col))
    return when(city_lower.like("%hồ chí minh%"), "HoChiMinh") \
        .when(city_lower.like("%hà nội%"), "HaNoi") \
        .otherwise(None)


def preprocess_data(df):
    """Comprehensive preprocessing for real estate data"""
    return df.withColumn("city_norm", normalize_city_name(col("thanh_pho"))) \
        .filter(col("city_norm").isin("HaNoi", "HoChiMinh")) \
        .withColumn("dien_tich", col("dien_tich").cast("float")) \
        .withColumn("gia_ban", col("gia_ban").cast("float")) \
        .withColumn("chieu_ngang", col("chieu_ngang").cast("float")) \
        .withColumn("chieu_dai", col("chieu_dai").cast("float")) \
        .withColumn("so_tang", col("so_tang").cast("int")) \
        .withColumn("so_phong_ngu", col("so_phong_ngu").cast("int")) \
        .na.fill({
        "duong_pho": "Unknown",
        "phuong_xa": "Unknown",
        "chieu_ngang": 0,
        "chieu_dai": 0,
        "so_tang": 0,
        "so_phong_ngu": 0
    }) \
        .filter((col("gia_ban") > 0) & (col("gia_ban") < 100)) \
        .filter((col("dien_tich") >= 10) & (col("dien_tich") < 1000)) \
        .withColumn("quan_huyen_clean",
                    regexp_replace(trim(lower(col("quan_huyen"))),
                                   "quận|quan|district|huyen|huyện", "")) \
        .withColumn("quan_huyen_clean", trim(col("quan_huyen_clean"))) \
        .withColumn("ngay_dang_parsed", to_date(col("ngay_dang"), "yyyy-MM-dd")) \
        .withColumn("year", year(col("ngay_dang_parsed"))) \
        .withColumn("month", month(col("ngay_dang_parsed"))) \
        .withColumn("year_month", concat(col("year"), lit("-"), col("month"))) \
        .withColumn("gia_m2_trieu", (col("gia_ban") * 1000) / col("dien_tich")) \
        .withColumn("shape_ratio",
                    when(col("chieu_ngang") > 0, col("chieu_dai") / col("chieu_ngang"))
                    .otherwise(0)) \
        .withColumn("rooms_per_100sqm", (col("so_phong_ngu") * 100) / col("dien_tich")) \
        .withColumn("dedup_key",
                    concat(
                        lower(trim(col("duong_pho"))),
                        lower(trim(col("quan_huyen"))),
                        col("dien_tich").cast("int"),
                        col("gia_ban")
                    )) \
        .dropDuplicates(["dedup_key"])


def time_series_analysis(df, city, output_dir):
    """Analyze time-based trends in pricing"""
    print(f"Performing time series analysis for {city}...")

    city_df = df.filter(col("city_norm") == city)

    # Create time window for monthly aggregation
    monthly_df = city_df.groupBy("year_month") \
        .agg(
        avg("gia_m2_trieu").alias("avg_price_m2"),
        count("*").alias("listing_count"),
        stddev("gia_m2_trieu").alias("price_std"),
        # SỬA ĐỔI Ở ĐÂY: Dùng expr()
        expr("percentile_approx(gia_m2_trieu, 0.5)").alias("median_price_m2")
    ) \
        .orderBy("year_month")

    # Prepare data for time series modeling
    windowSpec = Window.orderBy("year_month")
    trend_df = monthly_df \
        .withColumn("prev_month_price", lag("avg_price_m2", 1).over(windowSpec)) \
        .withColumn("price_change", col("avg_price_m2") - col("prev_month_price")) \
        .withColumn("price_change_pct",
                    when(col("prev_month_price").isNotNull() & (col("prev_month_price") > 0),
                         (col("price_change") / col("prev_month_price")) * 100)
                    .otherwise(0))

    # Save the results
    trend_df.coalesce(1).write.mode("overwrite") \
        .json(os.path.join(output_dir, "time_series", f"{city.lower()}_time_series.json"))

    # For ML predictions, convert to pandas
    try:
        ts_pd = trend_df.toPandas()
        ts_pd.to_csv(os.path.join(output_dir, "time_series", f"{city.lower()}_time_series.csv"), index=False)
    except Exception as e:
        print(f"Error exporting time series to CSV: {e}")

    return trend_df


# --- Các hàm phân tích khác (price_momentum_indicators, seasonality_detection, etc.) giữ nguyên ---
# --- vì chúng không sử dụng percentile_approx trong phiên bản bạn cung cấp ---

def price_momentum_indicators(df, city, output_dir):
    """Calculate price momentum indicators"""
    print(f"Calculating price momentum indicators for {city}...")

    city_df = df.filter(col("city_norm") == city)

    district_monthly = city_df.groupBy("quan_huyen_clean", "year_month") \
        .agg(avg("gia_m2_trieu").alias("avg_price_m2")) \
        .orderBy("quan_huyen_clean", "year_month")

    windowSpec = Window.partitionBy("quan_huyen_clean").orderBy("year_month")

    momentum_df = district_monthly \
        .withColumn("prev_1m", lag("avg_price_m2", 1).over(windowSpec)) \
        .withColumn("prev_3m", lag("avg_price_m2", 3).over(windowSpec)) \
        .withColumn("prev_6m", lag("avg_price_m2", 6).over(windowSpec)) \
        .withColumn("momentum_1m", col("avg_price_m2") - col("prev_1m")) \
        .withColumn("momentum_3m", col("avg_price_m2") - col("prev_3m")) \
        .withColumn("momentum_6m", col("avg_price_m2") - col("prev_6m")) \
        .withColumn("momentum_1m_pct",
                    when(col("prev_1m").isNotNull() & (col("prev_1m") > 0),
                         (col("momentum_1m") / col("prev_1m")) * 100)
                    .otherwise(0)) \
        .withColumn("momentum_3m_pct",
                    when(col("prev_3m").isNotNull() & (col("prev_3m") > 0),
                         (col("momentum_3m") / col("prev_3m")) * 100)
                    .otherwise(0)) \
        .withColumn("momentum_6m_pct",
                    when(col("prev_6m").isNotNull() & (col("prev_6m") > 0),
                         (col("momentum_6m") / col("prev_6m")) * 100)
                    .otherwise(0))

    latest_window = Window.partitionBy("quan_huyen_clean").orderBy(desc("year_month"))
    latest_momentum = momentum_df \
        .withColumn("rank", rank().over(latest_window)) \
        .filter(col("rank") == 1) \
        .drop("rank") \
        .orderBy(desc("momentum_3m_pct"))

    latest_momentum.coalesce(1).write.mode("overwrite") \
        .json(os.path.join(output_dir, "momentum", f"{city.lower()}_momentum.json"))

    return latest_momentum


def seasonality_detection(df, city, output_dir):
    print(f"Detecting seasonality for {city}...")
    city_df = df.filter(col("city_norm") == city)
    seasonal_df = city_df.groupBy("month") \
        .agg(
        avg("gia_m2_trieu").alias("avg_price_m2"),
        count("*").alias("listing_count"),
        stddev("gia_m2_trieu").alias("price_std")
    ) \
        .orderBy("month")

    yearly_avg_value = city_df.agg(avg("gia_m2_trieu")).collect()[0][0]
    if yearly_avg_value is None or yearly_avg_value == 0:  # Tránh chia cho 0 hoặc None
        print(f"Không thể tính yearly_avg_price cho {city}, bỏ qua tính seasonal_idx.")
        seasonal_with_idx = seasonal_df.withColumn("seasonal_idx", lit(None).cast("double")) \
            .withColumn("is_peak_season", lit(False)) \
            .withColumn("is_low_season", lit(False))
    else:
        seasonal_with_idx = seasonal_df \
            .withColumn("yearly_avg", lit(yearly_avg_value)) \
            .withColumn("seasonal_idx", (col("avg_price_m2") / col("yearly_avg")) * 100) \
            .withColumn("is_peak_season", col("seasonal_idx") > 105) \
            .withColumn("is_low_season", col("seasonal_idx") < 95)

    seasonal_with_idx.coalesce(1).write.mode("overwrite") \
        .json(os.path.join(output_dir, "seasonality", f"{city.lower()}_seasonality.json"))
    return seasonal_with_idx


def hotspot_identification(df, city, output_dir):
    print(f"Identifying hotspots for {city}...")
    city_df = df.filter(col("city_norm") == city)
    district_monthly = city_df.groupBy("quan_huyen_clean", "year_month") \
        .agg(
        avg("gia_m2_trieu").alias("avg_price_m2"),
        count("*").alias("listing_count")
    )

    window_earliest = Window.partitionBy("quan_huyen_clean").orderBy("year_month")
    window_latest = Window.partitionBy("quan_huyen_clean").orderBy(desc("year_month"))

    district_ranks = district_monthly \
        .withColumn("earliest_rank", rank().over(window_earliest)) \
        .withColumn("latest_rank", rank().over(window_latest))

    earliest_prices = district_ranks.filter(col("earliest_rank") == 1) \
        .select(col("quan_huyen_clean"), col("avg_price_m2").alias("earliest_price"),
                col("year_month").alias("earliest_month"))

    latest_prices = district_ranks.filter(col("latest_rank") == 1) \
        .select(col("quan_huyen_clean"), col("avg_price_m2").alias("latest_price"),
                col("year_month").alias("latest_month"), col("listing_count").alias("latest_count"))

    growth_df = latest_prices.join(earliest_prices, "quan_huyen_clean", "inner")  # Đảm bảo join thành công

    if growth_df.rdd.isEmpty():
        print(f"Không có đủ dữ liệu để tính toán growth cho {city}.")
        return spark.createDataFrame([], schema=latest_prices.schema.add("earliest_price", DoubleType()).add(
            "earliest_month", StringType()).add("price_growth", DoubleType()).add("growth_pct", DoubleType()).add(
            "is_hotspot", StringType()))

    growth_df = growth_df.withColumn("price_growth", col("latest_price") - col("earliest_price")) \
        .withColumn("growth_pct",
                    when(col("earliest_price").isNotNull() & (col("earliest_price") != 0),
                         ((col("latest_price") / col("earliest_price")) - 1) * 100)
                    .otherwise(None)  # hoặc 0 nếu muốn
                    ) \
        .withColumn("is_hotspot", col("growth_pct") > 20) \
        .orderBy(desc("growth_pct"))

    growth_df.coalesce(1).write.mode("overwrite") \
        .json(os.path.join(output_dir, "hotspots", f"{city.lower()}_hotspots.json"))
    return growth_df


def market_cycle_detection(df, city, output_dir):
    print(f"Analyzing market cycles for {city}...")
    city_df = df.filter(col("city_norm") == city)
    monthly_prices = city_df.groupBy("year_month") \
        .agg(avg("gia_m2_trieu").alias("avg_price_m2")) \
        .orderBy("year_month")

    if monthly_prices.count() < 12:  # Cần đủ dữ liệu cho MA dài nhất
        print(f"Không đủ dữ liệu hàng tháng cho {city} để phân tích chu kỳ thị trường.")
        # Trả về DataFrame rỗng với schema mong đợi
        return spark.createDataFrame([],
                                     schema=monthly_prices.schema.add("ma3", DoubleType()).add("ma6", DoubleType()).add(
                                         "ma12", DoubleType()).add("short_trend", StringType()).add("long_trend",
                                                                                                    StringType()).add(
                                         "market_phase", StringType()))

    windowSpec3 = Window.orderBy("year_month").rowsBetween(-2, 0)
    windowSpec6 = Window.orderBy("year_month").rowsBetween(-5, 0)
    windowSpec12 = Window.orderBy("year_month").rowsBetween(-11, 0)

    cycle_df = monthly_prices \
        .withColumn("ma3", avg("avg_price_m2").over(windowSpec3)) \
        .withColumn("ma6", avg("avg_price_m2").over(windowSpec6)) \
        .withColumn("ma12", avg("avg_price_m2").over(windowSpec12)) \
        .na.drop(subset=["ma12"])  # Bỏ các dòng đầu không đủ dữ liệu cho MA12

    if cycle_df.rdd.isEmpty():
        print(f"Không đủ dữ liệu sau khi tính MA cho {city}.")
        return spark.createDataFrame([],
                                     schema=monthly_prices.schema.add("ma3", DoubleType()).add("ma6", DoubleType()).add(
                                         "ma12", DoubleType()).add("short_trend", StringType()).add("long_trend",
                                                                                                    StringType()).add(
                                         "market_phase", StringType()))

    cycle_df = cycle_df.withColumn("short_trend",
                                   when(col("ma3") > col("ma6"), "up")
                                   .when(col("ma3") < col("ma6"), "down")
                                   .otherwise("flat")) \
        .withColumn("long_trend",
                    when(col("ma6") > col("ma12"), "up")
                    .when(col("ma6") < col("ma12"), "down")
                    .otherwise("flat")) \
        .withColumn("market_phase",
                    when((col("short_trend") == "up") & (col("long_trend") == "up"), "Strong Growth")
                    .when((col("short_trend") == "up") & (col("long_trend") == "down"), "Recovery")
                    .when((col("short_trend") == "down") & (col("long_trend") == "up"), "Slowing/Peak")
                    .when((col("short_trend") == "down") & (col("long_trend") == "down"), "Decline")
                    .otherwise("Stable"))

    cycle_df.coalesce(1).write.mode("overwrite") \
        .json(os.path.join(output_dir, "market_cycles", f"{city.lower()}_cycles.json"))
    return cycle_df


def create_economic_indicators(spark, output_dir):
    print("Creating mock economic indicators...")
    data = []
    start_year = 2020
    end_year = datetime.now().year
    for year_val in range(start_year, end_year + 1):
        for month_val in range(1, 13):
            # Giới hạn dữ liệu giả lập đến tháng hiện tại của năm hiện tại
            if year_val == end_year and month_val > datetime.now().month:
                break

            row = {
                "year_month": f"{year_val}-{str(month_val).zfill(2)}",  # Định dạng YYYY-MM
                "interest_rate": float(round(4 + np.sin(month_val / 6) + np.random.normal(0, 0.2), 2)),
                "inflation_rate": float(round(3 + 0.5 * np.sin(month_val / 3) + np.random.normal(0, 0.3), 2)),
                "unemployment": float(round(2 + 0.2 * np.sin(month_val / 12) + np.random.normal(0, 0.1), 2)),
                "gdp_growth": float(round(4.5 + np.sin(month_val / 4) + np.random.normal(0, 0.5), 2)),
                "stock_market_index": float(round(1000 + 50 * np.sin(month_val / 2) + np.random.normal(0, 20), 2))
            }
            data.append(row)

    economic_df = spark.createDataFrame(data)
    economic_df.coalesce(1).write.mode("overwrite") \
        .json(os.path.join(output_dir, "economic", "economic_indicators.json"))
    return economic_df


def correlation_analysis(df, economic_df, city, output_dir):
    print(f"Performing correlation analysis for {city}...")
    city_df = df.filter(col("city_norm") == city)
    monthly_prices = city_df.groupBy("year_month") \
        .agg(avg("gia_m2_trieu").alias("avg_price_m2"))

    # Đảm bảo year_month trong economic_df có cùng định dạng YYYY-MM
    economic_df_formatted = economic_df.withColumn("year_month",
                                                   date_format(to_date(col("year_month"), "yyyy-M"), "yyyy-MM"))

    combined_df = monthly_prices.join(economic_df_formatted, "year_month", "inner")

    if combined_df.count() < 2:  # Cần ít nhất 2 điểm dữ liệu để tính tương quan
        print(f"Không đủ dữ liệu chung cho {city} để tính toán tương quan.")
        return spark.createDataFrame([], schema=StructType(
            [StructField("factor", StringType()), StructField("correlation", DoubleType())])), \
            spark.createDataFrame([], schema=combined_df.schema)

    correlations_data = []
    factors = ["interest_rate", "inflation_rate", "unemployment", "gdp_growth", "stock_market_index"]
    for factor in factors:
        try:
            correlation_value = combined_df.stat.corr("avg_price_m2", factor)
            correlations_data.append(
                {"factor": factor, "correlation": correlation_value if correlation_value is not None else 0.0})
        except Exception as e_corr:
            print(f"Lỗi khi tính tương quan cho {factor} với {city}: {e_corr}")
            correlations_data.append({"factor": factor, "correlation": 0.0})  # Ghi 0 nếu có lỗi

    corr_df = spark.createDataFrame(correlations_data)

    corr_df.coalesce(1).write.mode("overwrite") \
        .json(os.path.join(output_dir, "correlations", f"{city.lower()}_correlations.json"))
    combined_df.coalesce(1).write.mode("overwrite") \
        .json(os.path.join(output_dir, "correlations", f"{city.lower()}_economic_combined.json"))
    return corr_df, combined_df


def create_visualizations(analysis_results, city, output_dir):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    viz_path = os.path.join(output_dir, "visualizations", city.lower())  # Thư mục riêng cho mỗi thành phố
    os.makedirs(viz_path, exist_ok=True)

    # Time series trend
    try:
        if "time_series" in analysis_results and not analysis_results["time_series"].rdd.isEmpty():
            time_series_pd = analysis_results["time_series"].toPandas()
            time_series_pd['year_month_dt'] = pd.to_datetime(time_series_pd["year_month"], format='%Y-%m')
            time_series_pd = time_series_pd.sort_values('year_month_dt')

            plt.figure(figsize=(12, 6))
            plt.plot(time_series_pd["year_month_dt"], time_series_pd["avg_price_m2"], marker='o', label='Giá TB/m2')
            if "median_price_m2" in time_series_pd.columns:
                plt.plot(time_series_pd["year_month_dt"], time_series_pd["median_price_m2"], marker='x', linestyle='--',
                         label='Giá Trung vị/m2')
            plt.title(f'Xu Hướng Giá / m² Theo Thời Gian - {city}')
            plt.xlabel('Năm-Tháng')
            plt.ylabel('Giá (Triệu VNĐ/m²)')
            plt.xticks(rotation=45)
            plt.legend()
            plt.tight_layout()
            plt.savefig(os.path.join(viz_path, f"price_trend_{timestamp}.png"))
            plt.close()
        else:
            print(f"Không có dữ liệu time_series cho {city} để vẽ biểu đồ.")
    except Exception as e:
        print(f"Lỗi khi tạo biểu đồ xu hướng thời gian cho {city}: {e}")

    # Seasonality
    try:
        if "seasonality" in analysis_results and not analysis_results["seasonality"].rdd.isEmpty():
            seasonality_pd = analysis_results["seasonality"].orderBy("month").toPandas()
            seasonality_pd["month"] = seasonality_pd["month"].astype(int)  # Đảm bảo tháng là số nguyên để sort
            seasonality_pd = seasonality_pd.sort_values("month")

            plt.figure(figsize=(10, 6))
            plt.bar(seasonality_pd["month"], seasonality_pd["seasonal_idx"])
            plt.axhline(y=100, color='r', linestyle='-')
            plt.title(f'Chỉ Số Giá Theo Mùa - {city}')
            plt.xlabel('Tháng')
            plt.ylabel('Chỉ số (100 = Trung bình năm)')
            plt.xticks(range(1, 13))
            plt.tight_layout()
            plt.savefig(os.path.join(viz_path, f"seasonality_{timestamp}.png"))
            plt.close()
        else:
            print(f"Không có dữ liệu seasonality cho {city} để vẽ biểu đồ.")
    except Exception as e:
        print(f"Lỗi khi tạo biểu đồ thời vụ cho {city}: {e}")

    # Hotspots
    try:
        if "hotspots" in analysis_results and not analysis_results["hotspots"].rdd.isEmpty():
            hotspots_pd = analysis_results["hotspots"].toPandas().sort_values("growth_pct", ascending=False).head(10)
            if not hotspots_pd.empty:
                plt.figure(figsize=(12, 7))  # Tăng chiều cao
                plt.bar(hotspots_pd["quan_huyen_clean"], hotspots_pd["growth_pct"])
                plt.title(f'Top 10 Quận/Huyện Tăng Trưởng Giá (%) - {city}')
                plt.xlabel('Quận/Huyện')
                plt.ylabel('Tăng Trưởng Giá (%)')
                plt.xticks(rotation=45, ha='right')
                plt.tight_layout()
                plt.savefig(os.path.join(viz_path, f"hotspots_{timestamp}.png"))
                plt.close()
            else:
                print(f"Không có dữ liệu hotspots cho {city} sau khi lọc top 10.")
        else:
            print(f"Không có dữ liệu hotspots cho {city} để vẽ biểu đồ.")
    except Exception as e:
        print(f"Lỗi khi tạo biểu đồ hotspots cho {city}: {e}")

    # Economic correlations
    try:
        if "correlations" in analysis_results and not analysis_results["correlations"].rdd.isEmpty():
            corr_pd = analysis_results["correlations"].toPandas()
            if not corr_pd.empty:
                plt.figure(figsize=(10, 6))
                plt.bar(corr_pd["factor"], corr_pd["correlation"])
                plt.title(f'Tương Quan với Các Yếu Tố Kinh Tế - {city}')
                plt.xlabel('Yếu Tố Kinh Tế')
                plt.ylabel('Hệ Số Tương Quan')
                plt.xticks(rotation=45, ha='right')
                plt.axhline(y=0, color='grey', linestyle='--')
                plt.tight_layout()
                plt.savefig(os.path.join(viz_path, f"correlations_{timestamp}.png"))
                plt.close()
            else:
                print(f"Không có dữ liệu tương quan cho {city}.")
        else:
            print(f"Không có dữ liệu correlations cho {city} để vẽ biểu đồ.")
    except Exception as e:
        print(f"Lỗi khi tạo biểu đồ tương quan cho {city}: {e}")

    print(f"Visualizations for {city} saved to {viz_path}")


if __name__ == "__main__":
    spark_app_name = "Comprehensive Real Estate Analysis"
    print(f"Initializing SparkSession: {spark_app_name}")

    spark = SparkSession.builder \
        .appName(spark_app_name) \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

try:
    # Setup paths
    # SỬ DỤNG ĐƯỜNG DẪN HDFS CHO INPUT
    input_path = "hdfs://hadoop-namenode:8020/user/kafka_data/*.json"

    # Output sẽ được lưu vào thư mục cục bộ bên trong container
    output_base = "/app/spark_jobs/output_comprehensive_analysis"  # Đường dẫn cục bộ trong container

    # Tạo các thư mục output nếu chưa tồn tại
    output_dirs_structure = {
        "time_series": None, "momentum": None, "seasonality": None,
        "hotspots": None, "market_cycles": None, "economic": None,
        "correlations": None, "visualizations": ["Hanoi", "HoChiMinh"]  # Thư mục con cho visualizations
    }

    for dir_name, sub_dirs in output_dirs_structure.items():
        base_dir_path = os.path.join(output_base, dir_name)
        os.makedirs(base_dir_path, exist_ok=True)
        print(f"Ensured directory exists: {base_dir_path}")
        if sub_dirs:
            for sub_dir in sub_dirs:
                sub_dir_path = os.path.join(base_dir_path, sub_dir.lower())
                os.makedirs(sub_dir_path, exist_ok=True)
                print(f"Ensured directory exists: {sub_dir_path}")

    print(f"Reading data files from HDFS: {input_path}")

    raw_df = spark.read.option("multiLine", "true").json(input_path)

    raw_count = raw_df.count()
    print(f"Read {raw_count} raw records from HDFS")
    if raw_count == 0:
        raise ValueError("No data read from HDFS. Stopping.")

    print("Data schema:")
    raw_df.printSchema()

    print("Preprocessing data...")
    processed_df = preprocess_data(raw_df)
    processed_df.cache()

    processed_count = processed_df.count()
    print(f"Data count after preprocessing: {processed_count}")
    if processed_count == 0:
        raise ValueError("No data after preprocessing. Stopping.")

    economic_df = create_economic_indicators(spark, output_base)

    hanoi_results = {}
    hcm_results = {}

    print("\n=== ANALYZING HANOI DATA ===")
    hanoi_results["time_series"] = time_series_analysis(processed_df, "HaNoi", output_base)
    hanoi_results["momentum"] = price_momentum_indicators(processed_df, "HaNoi", output_base)
    hanoi_results["seasonality"] = seasonality_detection(processed_df, "HaNoi", output_base)
    hanoi_results["hotspots"] = hotspot_identification(processed_df, "HaNoi", output_base)
    hanoi_results["market_cycles"] = market_cycle_detection(processed_df, "HaNoi", output_base)
    hanoi_results["correlations"], hanoi_results["econ_combined"] = correlation_analysis(
        processed_df, economic_df, "HaNoi", output_base)
    create_visualizations(hanoi_results, "Hanoi", output_base)

    print("\n=== ANALYZING HO CHI MINH CITY DATA ===")
    hcm_results["time_series"] = time_series_analysis(processed_df, "HoChiMinh", output_base)
    hcm_results["momentum"] = price_momentum_indicators(processed_df, "HoChiMinh", output_base)
    hcm_results["seasonality"] = seasonality_detection(processed_df, "HoChiMinh", output_base)
    hcm_results["hotspots"] = hotspot_identification(processed_df, "HoChiMinh", output_base)
    hcm_results["market_cycles"] = market_cycle_detection(processed_df, "HoChiMinh", output_base)
    hcm_results["correlations"], hcm_results["econ_combined"] = correlation_analysis(
        processed_df, economic_df, "HoChiMinh", output_base)
    create_visualizations(hcm_results, "HoChiMinh", output_base)

    print("\nGenerating summary report...")
    summary = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_records_analyzed": processed_count,
        "hanoi": {}, "hochiminh": {}
    }
    if not hanoi_results["hotspots"].rdd.isEmpty() and hanoi_results["hotspots"].count() > 0:
        summary["hanoi"]["hotspot_count"] = hanoi_results["hotspots"].filter(col("is_hotspot") == True).count()
        summary["hanoi"]["avg_price_growth"] = hanoi_results["hotspots"].agg(avg("growth_pct")).collect()[0][0]
    if not hanoi_results["market_cycles"].rdd.isEmpty() and hanoi_results["market_cycles"].count() > 0:
        summary["hanoi"]["market_phase"] = hanoi_results["market_cycles"].orderBy(desc("year_month")).first()[
            "market_phase"]

    if not hcm_results["hotspots"].rdd.isEmpty() and hcm_results["hotspots"].count() > 0:
        summary["hochiminh"]["hotspot_count"] = hcm_results["hotspots"].filter(col("is_hotspot") == True).count()
        summary["hochiminh"]["avg_price_growth"] = hcm_results["hotspots"].agg(avg("growth_pct")).collect()[0][0]
    if not hcm_results["market_cycles"].rdd.isEmpty() and hcm_results["market_cycles"].count() > 0:
        summary["hochiminh"]["market_phase"] = hcm_results["market_cycles"].orderBy(desc("year_month")).first()[
            "market_phase"]

    with open(os.path.join(output_base, "analysis_summary.json"), "w", encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    print("\nComprehensive analysis complete!")
    print(f"Results saved to local directory: {output_base}")

except Exception as e:
    print(f"Error in Spark processing: {e}")
    traceback.print_exc()
finally:
    if 'processed_df' in locals() and processed_df.is_cached:  # Kiểm tra is_cached
        processed_df.unpersist()
        print("Unpersisted processed_df.")
    if 'spark' in locals() and spark:
        spark.stop()
        print("SparkSession stopped.")
