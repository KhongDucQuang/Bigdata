# Yêu Cầu Hệ Thống

Trước khi triển khai hệ thống, hãy đảm bảo máy chủ của bạn đáp ứng các yêu cầu sau:

| Thành phần     | Yêu cầu                                           |
| -------------- | ------------------------------------------------- |
| Hệ điều hành   | Linux (khuyến nghị), macOS, hoặc Windows với WSL2 |
| Docker         | Phiên bản 19.03 hoặc cao hơn                      |
| Docker Compose | Phiên bản 1.27 hoặc cao hơn                       |
| CPU            | Tối thiểu 4 nhân (khuyến nghị 8 nhân trở lên)     |
| RAM            | Tối thiểu 16GB (khuyến nghị 32GB trở lên)         |
| Bộ nhớ lưu trữ | Tối thiểu 50GB dung lượng trống                   |
| Mạng           | Kết nối internet để tải về Docker images          |

## Các bước triển khai

### 1. Clone Repository

```bash
git clone https://github.com/KhongDucQuang/Bigdata.git
cd Bigdata
```

### 2. Cấu hình môi trường

Thay đổi các tham số cấu hình trong file `docker-compose.yml` nếu cần thiết. Một số biến môi trường quan trọng:

| Dịch vụ                 | Tham số cấu hình             | Giá trị mặc định         | Mô tả                           |
| ----------------------- | ---------------------------- | ------------------------ | ------------------------------- |
| Kafka                   | KAFKA\_BROKER\_ID            | 1                        | ID duy nhất cho Kafka broker    |
| Kafka                   | KAFKA\_LISTENERS             | PLAINTEXT://0.0.0.0:9092 | Cấu hình listener               |
| Kafka                   | KAFKA\_ADVERTISED\_LISTENERS | PLAINTEXT://kafka:9092   | Địa chỉ client dùng để kết nối  |
| Elasticsearch           | discovery.type               | single-node              | Chạy như một node đơn           |
| Spark Streaming Jobs    | KAFKA\_TOPIC                 | real-estate-topic        | Kafka topic để tiêu thụ dữ liệu |
| Spark Streaming Anomaly | ANOMALY\_THRESHOLD\_PERCENT  | 0.30                     | Ngưỡng phát hiện bất thường     |

### 3. Xây dựng và khởi động dịch vụ

```bash
docker-compose up -d
```

Khởi chạy lần đầu sẽ mất thời gian vì cần tải về và build các image cần thiết.

Để build riêng một dịch vụ:

```bash
docker-compose build <tên-dịch-vụ>
```

Ví dụ: kafka-hdfs-consumer, kafka-producer, spark-batch-runner, v.v.

---

## Thứ tự khởi động dịch vụ

Docker Compose tự động xử lý dependencies giữa các dịch vụ theo thứ tự:
Zookeeper → Kafka → Kafka HDFS Consumer → Kafka Producer → Hadoop NameNode → Hadoop DataNode → Spark Master → Spark Worker → Spark Batch Runner → Spark Streaming Job → Spark Streaming Median District → Spark Streaming Anomaly → Elasticsearch → Kibana

## Cấu hình container

### Lưu trữ và xử lý dữ liệu

| Container       | Image                                                | Cổng       | Volume                               | Mục đích              |
| --------------- | ---------------------------------------------------- | ---------- | ------------------------------------ | --------------------- |
| zookeeper       | confluentinc/cp-zookeeper:7.4.0                      | 2181       | -                                    | Dịch vụ đồng bộ       |
| kafka           | confluentinc/cp-kafka:7.4.0                          | 9092       | -                                    | Broker tin nhắn       |
| hadoop-namenode | bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8      | 9870       | namenode:/hadoop/dfs/name            | Quản lý metadata HDFS |
| hadoop-datanode | bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8      | -          | datanode:/hadoop/dfs/data            | Lưu trữ block dữ liệu |
| spark-master    | bde2020/spark-master:2.4.4-hadoop2.7                 | 7077, 8080 | ./spark\_jobs\:/app/spark\_jobs      | Quản lý job Spark     |
| spark-worker    | bde2020/spark-worker:2.4.4-hadoop2.7                 | 8081       | -                                    | Thực thi job Spark    |
| elasticsearch   | docker.elastic.co/elasticsearch/elasticsearch:8.13.4 | 9200       | esdata:/usr/share/elasticsearch/data | Tìm kiếm dữ liệu      |

### Ứng dụng

| Container                       | Build Context      | Phụ thuộc                     | Chính sách khởi động | Mục đích                     |
| ------------------------------- | ------------------ | ----------------------------- | -------------------- | ---------------------------- |
| kafka-hdfs-consumer             | ./kafka\_consumer  | kafka, hadoop-namenode        | on-failure           | Lưu Kafka message vào HDFS   |
| kafka-producer                  | ./kafka\_producer  | kafka-hdfs-consumer           | default              | Sinh dữ liệu vào Kafka       |
| spark-batch-runner              | . (root)           | spark-master, hadoop-namenode | no                   | Chạy job xử lý batch         |
| spark-streaming-job             | ./spark\_streaming | kafka, elasticsearch          | on-failure           | Xử lý dữ liệu real-time      |
| spark-streaming-median-district | ./spark\_streaming | kafka, elasticsearch          | on-failure           | Tính giá trung vị theo quận  |
| spark-streaming-anomaly         | ./spark\_streaming | kafka, elasticsearch          | on-failure           | Phát hiện dữ liệu bất thường |
| kibana                          | kibana:8.13.4      | elasticsearch                 | default              | Giao diện trực quan hóa      |

---

## Quản lý Volume

| Volume   | Mục đích              | Dùng bởi        |
| -------- | --------------------- | --------------- |
| namenode | Metadata HDFS         | hadoop-namenode |
| datanode | Block dữ liệu HDFS    | hadoop-datanode |
| esdata   | Dữ liệu Elasticsearch | elasticsearch   |

## Giám sát triển khai

| Dịch vụ         | URL                                            | Mục đích                     |
| --------------- | ---------------------------------------------- | ---------------------------- |
| Hadoop NameNode | [http://localhost:9870](http://localhost:9870) | Kiểm tra trạng thái HDFS     |
| Spark Master    | [http://localhost:8080](http://localhost:8080) | Theo dõi job Spark và worker |
| Spark Worker    | [http://localhost:8081](http://localhost:8081) | Trạng thái worker            |
| Elasticsearch   | [http://localhost:9200](http://localhost:9200) | API tìm kiếm dữ liệu         |
| Kibana          | [http://localhost:5601](http://localhost:5601) | Dashboard trực quan hóa      |

## Quản lý dịch vụ

### Kiểm tra trạng thái

```bash
docker-compose ps
```

### Xem logs

```bash
docker-compose logs        # Tất cả

docker-compose logs <service-name> # Cụ thể
```

### Dừng hệ thống

```bash
docker-compose down        # Dừng không xóa dữ liệu

docker-compose down -v     # Dừng và xóa volume
```

### Khởi động lại dịch vụ riêng lẻ

```bash
docker-compose restart <service-name>
```

---

## Khắc phục lỗi thường gặp

| Lỗi                      | Nguyên nhân                   | Giải pháp                               |
| ------------------------ | ----------------------------- | --------------------------------------- |
| Kafka không khởi chạy    | Zookeeper chưa sẵn sàng       | Khởi động lại Zookeeper                 |
| HDFS lỗi                 | Namenode chưa init            | Kiểm tra log Namenode                   |
| Job Spark lỗi            | Không kết nối Kafka hoặc HDFS | Kiểm tra mạng Docker                    |
| Elasticsearch không chạy | Thiếu RAM Docker              | Tăng giới hạn bộ nhớ Docker             |
| Streaming lỗi            | Lỗi cấu hình                  | Kiểm tra biến môi trường docker-compose |

---

## Thông tin build Image tùy chỉnh

### Spark Batch Runner Image

* Base image: Bitnami Spark 2.4.4
* Cài Python 3.7, cấu hình môi trường cho Hadoop và Spark
* Copy code job, cấu hình Ivy cache, sử dụng user root
* Script entrypoint chạy job batch

---

## Cấu hình mạng nội bộ

* Mạng Docker: `lambda_net`
* Giao tiếp nội bộ qua tên dịch vụ
* Port chỉ mở ra ngoài khi cần thiết
