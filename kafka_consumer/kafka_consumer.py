import json
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from hdfs import InsecureClient
from hdfs.util import HdfsError
import logging
import time
from datetime import datetime
import os
import signal

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - PID:%(process)d - %(message)s')
log = logging.getLogger(__name__)

# --- Cấu hình ---
KAFKA_BROKER_URL = 'kafka:9092'
KAFKA_TOPIC = 'real-estate-topic'  # Hoặc 'alonhadat' tùy bạn chọn
KAFKA_GROUP_ID = 'hdfs-writer-group-dynamic-final-v1'  # Thay đổi để reset offset nếu cần

HDFS_URL = 'http://hadoop-namenode:9870'
HDFS_USER = 'root'
HDFS_BASE_PATH = '/user/kafka_data/real_estate_by_date'  # Hoặc /user/root/realestate_data/raw/alonhadat

BATCH_SIZE = 100
BATCH_INTERVAL_SECONDS = 60

# --- Khai báo biến toàn cục cho các client và trạng thái ---
# Chúng sẽ được khởi tạo trong vòng lặp của __main__
hdfs_client = None
consumer = None
hdfs_ready = False


# --- Hàm ghi batch vào file HDFS mới ---
def write_data_to_new_hdfs_file(current_hdfs_client, base_hdfs_path, batch_data_list):
    global hdfs_ready, hdfs_client  # Để có thể đặt lại nếu lỗi nghiêm trọng
    if not batch_data_list:
        log.info("CONSUMER_DEBUG: Batch rỗng, không có gì để ghi vào HDFS.")
        return True
    if not current_hdfs_client:
        log.error("CONSUMER_ERROR: HDFS client không sẵn sàng (None) khi gọi write_data_to_new_hdfs_file.")
        hdfs_ready = False  # Đánh dấu HDFS không sẵn sàng
        return False

    now = datetime.now()
    date_path_part = now.strftime('%Y/%m/%d')
    hdfs_target_dir = f"{base_hdfs_path}/{date_path_part}"
    filename = f"data_{now.strftime('%H%M%S%f')}.jsonl"
    full_hdfs_path = f"{hdfs_target_dir}/{filename}"

    log.info(f"CONSUMER_INFO: Chuẩn bị ghi {len(batch_data_list)} bản ghi vào file HDFS mới: {full_hdfs_path}")
    try:
        current_hdfs_client.makedirs(hdfs_target_dir)
        log.info(f"CONSUMER_DEBUG: Đã đảm bảo thư mục HDFS tồn tại: {hdfs_target_dir}")
        jsonl_data_string = "\n".join(json.dumps(record, ensure_ascii=False) for record in batch_data_list)
        with current_hdfs_client.write(full_hdfs_path, encoding='utf-8', overwrite=False) as writer:
            writer.write(jsonl_data_string)
        log.info(f"CONSUMER_SUCCESS: Đã ghi thành công {len(batch_data_list)} bản ghi vào {full_hdfs_path}")
        return True
    except HdfsError as he:
        log.error(f"CONSUMER_ERROR: Lỗi HDFS khi ghi vào file {full_hdfs_path}: {he}")
        # Nếu lỗi HDFS nghiêm trọng (ví dụ: không thể kết nối, safe mode kéo dài), đặt lại client
        hdfs_client = None  # Sẽ được khởi tạo lại ở vòng lặp __main__
        hdfs_ready = False
        return False
    except Exception as e:
        log.error(f"CONSUMER_ERROR: Lỗi không xác định khi ghi vào HDFS {full_hdfs_path}: {e}", exc_info=True)
        hdfs_client = None  # Sẽ được khởi tạo lại ở vòng lặp __main__
        hdfs_ready = False
        return False


# --- Hàm chính để tiêu thụ và ghi dữ liệu ---
def consume_and_write(MAX_TIME_INTERVAL=60):
    # Sử dụng các biến global đã được khởi tạo (hoặc sẽ được khởi tạo lại) bởi vòng lặp trong __main__
    global hdfs_client, consumer, hdfs_ready

    # Kiểm tra lại lần nữa nếu các client đã sẵn sàng chưa (phòng trường hợp)
    if not (hdfs_ready and consumer and hdfs_client):
        log.error("CONSUMER_CRITICAL: consume_and_write được gọi nhưng HDFS hoặc Kafka client chưa sẵn sàng.")
        # Đặt hdfs_ready = False để vòng lặp __main__ bên ngoài biết cần khởi tạo lại mọi thứ
        hdfs_ready = False
        if consumer:  # Đóng consumer nếu nó tồn tại nhưng hdfs thì không
            try:
                consumer.close()
            except:
                pass
        consumer = None  # Đảm bảo consumer cũng được khởi tạo lại
        return  # Kết thúc phiên làm việc này của consume_and_write

    log.info("CONSUMER_INFO: Bắt đầu lắng nghe message từ Kafka...")
    message_batch = []
    last_write_time = time.time()

    try:
        while True:  # Vòng lặp chính bên trong consume_and_write
            # Kiểm tra client trước mỗi vòng lặp con, nếu có lỗi thì thoát ra để __main__ xử lý
            if not hdfs_ready or hdfs_client is None:
                log.warning(
                    "CONSUMER_WARNING: HDFS client không sẵn sàng giữa chừng (trong consume_and_write). Thoát phiên làm việc.")
                hdfs_ready = False  # Báo hiệu cho vòng lặp __main__
                if consumer: consumer.close(); consumer = None  # Đóng và báo hiệu cho vòng lặp __main__
                return  # Thoát khỏi consume_and_write

            if consumer is None:
                log.warning(
                    "CONSUMER_WARNING: Kafka consumer là None giữa chừng (trong consume_and_write). Thoát phiên làm việc.")
                hdfs_ready = False  # Để __main__ khởi tạo lại cả HDFS nếu cần
                return  # Thoát khỏi consume_and_write

            log.info(
                "CONSUMER_DEBUG: Chuẩn bị vào vòng lặp 'for message in consumer'. Số message trong batch hiện tại: %d",
                len(message_batch))
            messages_in_current_poll = 0
            try:
                for message in consumer:  # Vòng này sẽ timeout sau consumer_timeout_ms
                    messages_in_current_poll += 1
                    record = message.value
                    log.info(
                        f"CONSUMER_DEBUG: Nhận được message: Partition={message.partition}, Offset={message.offset}")
                    message_batch.append(record)

                    current_time = time.time()
                    if len(message_batch) >= BATCH_SIZE or \
                            (len(message_batch) > 0 and current_time - last_write_time >= BATCH_INTERVAL_SECONDS):
                        log.info("CONSUMER_DEBUG: Điều kiện ghi batch (trong vòng lặp message) được thỏa mãn.")
                        write_success = write_data_to_new_hdfs_file(hdfs_client, HDFS_BASE_PATH, message_batch)
                        if write_success:
                            message_batch = []
                            last_write_time = current_time
                        else:
                            log.error(
                                "CONSUMER_ERROR: Ghi batch vào HDFS thất bại. Thoát phiên làm việc để thử lại từ đầu.")
                            # hdfs_client và hdfs_ready đã được đặt lại trong write_data_to_new_hdfs_file nếu có lỗi
                            if consumer: consumer.close(); consumer = None  # Đóng và báo hiệu cho __main__
                            return  # Thoát consume_and_write

            except HdfsError as he_poll:  # Bắt lỗi HDFS có thể xảy ra khi thư viện hdfs ngầm tương tác (ít xảy ra ở đây)
                log.error(f"CONSUMER_ERROR: Lỗi HDFS trong lúc poll message: {he_poll}", exc_info=True)
                hdfs_client = None;
                hdfs_ready = False
                if consumer: consumer.close(); consumer = None
                return  # Thoát consume_and_write
            except Exception as e_poll:
                log.error(f"CONSUMER_ERROR: Lỗi trong lúc poll message từ Kafka: {e_poll}", exc_info=True)
                # Nếu lỗi Kafka nghiêm trọng, đóng consumer và thoát để __main__ thử lại
                if consumer: consumer.close(); consumer = None
                hdfs_ready = False  # Cũng nên reset HDFS vì không rõ trạng thái
                return  # Thoát consume_and_write

            log.info(
                "CONSUMER_DEBUG: Thoát khỏi vòng lặp 'for message in consumer'. Nhận được %d message trong lần poll này. Tổng message trong batch: %d",
                messages_in_current_poll, len(message_batch))

            if not hdfs_ready:  # Nếu HDFS bị lỗi trong lúc ghi batch ở trên (đã được set bởi write_data_to_new_hdfs_file)
                log.warning("CONSUMER_WARNING: HDFS không sẵn sàng sau khi thử ghi batch. Thoát phiên làm việc.")
                if consumer: consumer.close(); consumer = None
                return  # Thoát consume_and_write

            current_time = time.time()
            if len(message_batch) > 0 and (current_time - last_write_time >= MAX_TIME_INTERVAL):
                log.info("CONSUMER_DEBUG: Điều kiện ghi batch (sau vòng lặp message, theo thời gian) được thỏa mãn.")
                write_success = write_data_to_new_hdfs_file(hdfs_client, HDFS_BASE_PATH, message_batch)
                if write_success:
                    message_batch = []
                    last_write_time = current_time
                else:
                    log.error(
                        "CONSUMER_ERROR: Ghi batch (sau vòng lặp message) vào HDFS thất bại. Thoát phiên làm việc.")
                    # hdfs_client, hdfs_ready đã được xử lý trong write_data_to_new_hdfs_file
                    if consumer: consumer.close(); consumer = None
                    return  # Thoát consume_and_write

            if messages_in_current_poll == 0 and not message_batch:
                log.info("CONSUMER_DEBUG: Không có message mới và batch rỗng, nghỉ 1 giây.")
                time.sleep(1)

    except KeyboardInterrupt:
        log.warning("CONSUMER_WARNING: Nhận tín hiệu dừng (Ctrl+C) trong consume_and_write.")
        # Sẽ được xử lý bởi shutdown_handler hoặc khối finally bên ngoài nếu có
        raise  # Ném lại để shutdown_handler bắt hoặc khối __main__ xử lý
    except Exception as e_main_loop:
        log.error(f"CONSUMER_ERROR: Lỗi nghiêm trọng trong vòng lặp chính của consume_and_write: {e_main_loop}",
                  exc_info=True)
        # Đặt lại tất cả để __main__ thử lại
        hdfs_client = None;
        hdfs_ready = False
        if consumer: consumer.close(); consumer = None
        return  # Thoát consume_and_write
    # Khối finally của consume_and_write chỉ nên ghi batch cuối nếu không có lỗi nghiêm trọng
    # Việc đóng client sẽ do __main__ hoặc shutdown_handler đảm nhiệm khi hàm này thoát
    # Tuy nhiên, để an toàn, nếu thoát bình thường (không phải do exception), cũng nên ghi batch cuối
    finally:
        log.info("CONSUMER_INFO: Kết thúc một phiên làm việc của consume_and_write.")
        # Ghi nốt batch cuối nếu thoát bình thường mà không phải do KeyboardInterrupt và client còn tốt
        # KeyboardInterrupt sẽ được xử lý bởi shutdown_handler
        # Các lỗi khác khiến client hỏng thì nên để __main__ khởi tạo lại từ đầu
        if message_batch and hdfs_client and hdfs_ready and not isinstance(
                e_main_loop if 'e_main_loop' in locals() else None, KeyboardInterrupt):
            log.info(
                f"CONSUMER_INFO: Đang ghi nốt {len(message_batch)} messages cuối cùng (từ finally của consume_and_write)...")
            write_data_to_new_hdfs_file(hdfs_client, HDFS_BASE_PATH, message_batch)


# --- Xử lý tín hiệu dừng ---
def shutdown_handler(signum, frame):
    global consumer, hdfs_client, hdfs_ready, message_batch  # Cần truy cập message_batch nếu nó toàn cục
    # Hoặc truyền nó vào đây, hoặc logic ghi batch cuối phải khác
    log.warning(f"APP_INFO: Nhận được tín hiệu {signal.Signals(signum).name}. Đang dừng ứng dụng...")

    # Ghi nốt batch cuối cùng nếu có và client còn tốt
    # Biến message_batch hiện tại là local của consume_and_write, nên cách này không truy cập được.
    # Cách tốt nhất là để khối finally của consume_and_write xử lý batch cuối khi KeyboardInterrupt.
    # Hoặc consume_and_write trả về message_batch khi thoát để __main__ xử lý.
    # Hiện tại, chúng ta dựa vào KeyboardInterrupt sẽ kích hoạt finally trong consume_and_write (nếu nó đang ở đó)
    # hoặc finally trong __main__ (nếu có).
    # Đoạn này chỉ đóng consumer.

    if consumer:
        try:
            consumer.close()
            log.info("APP_INFO: Kafka Consumer đã được đóng từ shutdown_handler.")
        except Exception as e_sh:
            log.error(f"APP_ERROR: Lỗi khi đóng consumer từ shutdown_handler: {e_sh}")
    exit(0)


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

# --- Bắt đầu chạy ---
if __name__ == "__main__":
    main_retry_delay = 15
    while True:
        hdfs_ready = False
        hdfs_client = None
        consumer = None
        log.info(f"APP_INFO: Bắt đầu vòng lặp khởi tạo chính. Thử lại sau {main_retry_delay} giây nếu có lỗi.")

        temp_hdfs_retries = 0
        max_hdfs_retries = 3
        while not hdfs_ready and temp_hdfs_retries < max_hdfs_retries:
            if hdfs_client is None:
                try:
                    log.info(
                        f"APP_INFO_HDFS: Đang kết nối tới HDFS tại {HDFS_URL} với user {HDFS_USER} (lần thử {temp_hdfs_retries + 1})")
                    hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)
                    if not hdfs_client.status(HDFS_BASE_PATH, strict=False):
                        log.info(f"APP_INFO_HDFS: Thư mục cơ sở {HDFS_BASE_PATH} chưa tồn tại. Đang tạo...")
                        hdfs_client.makedirs(HDFS_BASE_PATH)
                        log.info(f"APP_INFO_HDFS: Đã tạo thư mục cơ sở {HDFS_BASE_PATH}.")
                    else:
                        log.info(f"APP_INFO_HDFS: Thư mục cơ sở {HDFS_BASE_PATH} đã tồn tại.")
                    log.info(f"APP_SUCCESS_HDFS: Kết nối HDFS thành công. Thư mục cơ sở: {HDFS_BASE_PATH}.")
                    hdfs_ready = True
                    break  # Thoát vòng lặp HDFS
                except HdfsError as he:
                    log.error(f"APP_ERROR_HDFS: Lỗi HDFS khi kết nối/tạo thư mục {HDFS_BASE_PATH}: {he}")
                except Exception as e:
                    log.error(f"APP_ERROR_HDFS: Lỗi không xác định khi kết nối HDFS: {e}", exc_info=True)

            temp_hdfs_retries += 1
            if not hdfs_ready and temp_hdfs_retries < max_hdfs_retries:
                log.info(f"APP_INFO_HDFS: Đặt lại HDFS client và thử lại sau 10 giây...")
                hdfs_client = None
                time.sleep(10)
            elif not hdfs_ready:
                log.error(f"APP_ERROR_HDFS: Không thể kết nối HDFS sau {max_hdfs_retries} lần thử.")

        if hdfs_ready:
            temp_kafka_retries = 0
            max_kafka_retries = 3
            while consumer is None and temp_kafka_retries < max_kafka_retries:
                try:
                    log.info(
                        f"APP_INFO_KAFKA: Đang kết nối Kafka Consumer tới brokers: {KAFKA_BROKER_URL}, topic: {KAFKA_TOPIC}, group: {KAFKA_GROUP_ID} (lần thử {temp_kafka_retries + 1})")
                    consumer = KafkaConsumer(
                        KAFKA_TOPIC,
                        bootstrap_servers=KAFKA_BROKER_URL,
                        auto_offset_reset='earliest',
                        group_id=KAFKA_GROUP_ID,
                        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                        consumer_timeout_ms=10000,
                        enable_auto_commit=True,
                        auto_commit_interval_ms=5000
                    )
                    log.info(f"APP_SUCCESS_KAFKA: Đã kết nối tới Kafka broker.")
                    break
                except NoBrokersAvailable as nba_err:
                    log.error(f"APP_ERROR_KAFKA: Không thể kết nối tới Kafka brokers: {nba_err}.")
                except Exception as e:
                    log.error(f"APP_ERROR_KAFKA: Lỗi không xác định khi khởi tạo Kafka Consumer: {e}", exc_info=True)

                temp_kafka_retries += 1
                if consumer is None and temp_kafka_retries < max_kafka_retries:
                    log.info(f"APP_INFO_KAFKA: Thử lại kết nối Kafka sau 10 giây...")
                    time.sleep(10)
                elif consumer is None:
                    log.error(f"APP_ERROR_KAFKA: Không thể kết nối Kafka sau {max_kafka_retries} lần thử.")

        if hdfs_ready and consumer:
            try:
                log.info("APP_INFO: Khởi chạy phiên làm việc consume_and_write...")
                consume_and_write()
                log.info("APP_INFO: Phiên làm việc consume_and_write đã kết thúc.")
            except KeyboardInterrupt:
                log.warning("APP_WARNING: KeyboardInterrupt bắt được ở vòng lặp __main__. Đang thoát...")
                break
            except Exception as main_exc:
                log.error(
                    f"APP_ERROR: Lỗi không mong muốn ở vòng lặp __main__ sau khi gọi consume_and_write: {main_exc}",
                    exc_info=True)
        else:
            log.error("APP_ERROR: Không thể khởi tạo HDFS hoặc Kafka client. Sẽ thử lại toàn bộ.")

        log.warning(f"APP_WARNING: Kết thúc một chu kỳ của vòng lặp __main__. Thử lại sau {main_retry_delay} giây...")
        time.sleep(main_retry_delay)