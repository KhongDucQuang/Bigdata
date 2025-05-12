import json
from kafka import KafkaConsumer
from hdfs import InsecureClient
import logging
import time
import os # Sử dụng để lấy Process ID cho tên file HDFS duy nhất nếu cần

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - PID:%(process)d - %(message)s')

# --- Cấu hình ---
KAFKA_BROKER_URL = 'kafka:9092'
KAFKA_TOPIC = 'real-estate-topic'
HDFS_URL = 'http://hadoop-namenode:9870'
HDFS_USER = 'root'

# QUAN TRỌNG: Chiến lược đặt tên file HDFS
# Nếu bạn dự định chạy NHIỀU INSTANCE của script này để tăng thông lượng,
# mỗi instance PHẢI ghi vào một file HDFS RIÊNG BIỆT để tránh lease conflict.
# Cách 1: Mỗi instance ghi vào file riêng dựa trên PID (Process ID)
# HDFS_FILE_BASENAME = 'real_estate_data'
# HDFS_PATH_DIR = '/user/kafka_data'
# HDFS_FILENAME = f'{HDFS_FILE_BASENAME}_pid{os.getpid()}.jsonl'
# HDFS_PATH = f'{HDFS_PATH_DIR}/{HDFS_FILENAME}'
# Sau đó bạn sẽ cần một job khác để hợp nhất các file này.

# Cách 2: CHỈ CHẠY MỘT INSTANCE của script này, ghi vào một file cố định.
# Đây là cách được giả định trong phần còn lại của code.
# Nếu bạn chọn cách này, hãy đảm bảo chỉ một instance chạy.
HDFS_PATH_DIR = '/user/kafka_data'
HDFS_FILENAME = 'data_batch_test.jsonl'
HDFS_PATH = f'{HDFS_PATH_DIR}/{HDFS_FILENAME}'


# Cấu hình batching
MAX_BATCH_SIZE = 100  # Số lượng message tối đa trong một batch
MAX_TIME_INTERVAL = 60 # Thời gian tối đa (giây) trước khi ghi batch, ngay cả khi chưa đủ MAX_BATCH_SIZE

# --- Khởi tạo Client ---
hdfs_client = None
consumer = None
hdfs_ready = False

# --- Hàm phụ trợ để ghi batch ---
def write_batch_to_hdfs(client, path, batch_data):
    if not batch_data:
        return
    try:
        # Kiểm tra thư mục tồn tại, tạo nếu chưa có (thêm bước này vào hàm ghi batch cho chắc chắn)
        current_hdfs_dir = "/".join(path.split("/")[:-1])
        if not client.status(current_hdfs_dir, strict=False):
            client.makedirs(current_hdfs_dir)
            logging.info(f"Đã tạo thư mục {current_hdfs_dir} trong HDFS (trong lúc ghi batch)")

        # Kiểm tra file tồn tại, tạo file rỗng nếu ghi lần đầu tiên vào file này
        # Hoặc nếu file đã bị xóa vì lý do nào đó sau lần kiểm tra ban đầu
        if not client.status(path, strict=False):
            logging.info(f"File {path} không tìm thấy TRƯỚC KHI GHI BATCH. Tạo file rỗng...")
            with client.write(path, overwrite=True, encoding='utf-8') as writer:
                writer.write("") # Tạo file rỗng


        with client.write(path, append=True, encoding='utf-8') as writer:
            for item_json_string in batch_data:
                writer.write(item_json_string + '\n')
        logging.info(f"Đã ghi {len(batch_data)} messages vào HDFS: {path}")
    except Exception as e:
        logging.error(f"Lỗi nghiêm trọng khi ghi batch vào HDFS ({path}): {e}. {len(batch_data)} messages có thể bị mất hoặc cần xử lý lại.")
        # Trong môi trường production, bạn có thể muốn:
        # 1. Thử lại ghi batch này sau một khoảng thời gian.
        # 2. Ghi batch này vào một "dead letter queue" hoặc file cục bộ để xử lý sau.
        # 3. Reset hdfs_client và thử kết nối lại ở vòng lặp chính bên ngoài.
        raise # Ném lại lỗi để vòng lặp chính có thể xử lý (ví dụ: reset client)

# --- Vòng lặp kiểm tra HDFS sẵn sàng ---
while not hdfs_ready:
    if hdfs_client is None:
        try:
            hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)
            logging.info(f"Đã kết nối tới HDFS tại {HDFS_URL} với user {HDFS_USER}")
        except Exception as e:
            logging.error(f"Không thể kết nối tới HDFS: {e}. Đang thử lại sau 10 giây...")
            hdfs_client = None
            time.sleep(10)
            continue

    try:
        # Tạo thư mục nếu chưa tồn tại
        if not hdfs_client.status(HDFS_PATH_DIR, strict=False):
             hdfs_client.makedirs(HDFS_PATH_DIR)
             logging.info(f"Đã tạo thư mục {HDFS_PATH_DIR} trong HDFS")

        # Kiểm tra xem FILE có tồn tại không, tạo nếu chưa có
        if not hdfs_client.status(HDFS_PATH, strict=False):
            logging.info(f"File {HDFS_PATH} chưa tồn tại. Đang tạo file rỗng...")
            with hdfs_client.write(HDFS_PATH, overwrite=True, encoding='utf-8') as writer:
                writer.write("") # Ghi chuỗi rỗng để tạo file
            logging.info(f"Đã tạo file rỗng thành công: {HDFS_PATH}")
            hdfs_ready = True
        else:
            # File đã tồn tại, thử ghi nối tiếp một chuỗi rỗng để kiểm tra quyền
            logging.info(f"File {HDFS_PATH} đã tồn tại. Kiểm tra quyền ghi (append)...")
            with hdfs_client.write(HDFS_PATH, append=True, encoding='utf-8') as writer:
                 writer.write("") # Thử ghi nối tiếp chuỗi rỗng
            logging.info(f"Kiểm tra ghi (append) thành công vào HDFS path: {HDFS_PATH}")
            hdfs_ready = True

    except Exception as hdfs_err:
         logging.error(f"Lỗi khi kiểm tra/tạo file hoặc thư mục HDFS: {hdfs_err}")
         logging.info("Đặt lại HDFS client và thử lại sau 10 giây...")
         if hdfs_client:
             try:
                 # Cố gắng đóng client hiện tại một cách an toàn nếu có
                 # thư viện hdfs không có phương thức close() rõ ràng cho InsecureClient,
                 # nhưng nếu có lỗi, việc gán None và tạo mới là cách làm phổ biến.
                 pass
             except Exception as ex_close:
                 logging.warning(f"Lỗi khi cố gắng đóng hdfs_client cũ: {ex_close}")
         hdfs_client = None # Quan trọng: reset client để vòng lặp trên tạo lại
         time.sleep(10)


# --- Khởi tạo Kafka Consumer (Chỉ chạy sau khi hdfs_ready = True) ---
while consumer is None:
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER_URL,
            auto_offset_reset='earliest', # Bắt đầu đọc từ message cũ nhất nếu consumer group mới
            group_id='hdfs-writer-group-batched-v1', # Thay đổi group_id nếu logic thay đổi đáng kể
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            consumer_timeout_ms=10000, # Timeout 10 giây, để vòng lặp không bị block vô hạn nếu không có message
            enable_auto_commit=True, # Bật auto commit offset Kafka sau khi message được consumer lấy về
            auto_commit_interval_ms=5000 # Tần suất auto commit
        )
        logging.info(f"Đã kết nối tới Kafka broker tại {KAFKA_BROKER_URL} và đăng ký topic {KAFKA_TOPIC}")
    except Exception as e:
        logging.error(f"Không thể kết nối tới Kafka: {e}. Đang thử lại sau 10 giây...")
        time.sleep(10)

# --- Vòng lặp xử lý message ---
logging.info("Bắt đầu lắng nghe message từ Kafka...")
message_batch = []
last_write_time = time.time()

try:
    while True: # Vòng lặp chính để duy trì việc lắng nghe
        for message in consumer: # consumer_timeout_ms sẽ làm vòng này thoát ra nếu không có message
            try:
                record = message.value
                # logging.info(f"Nhận: P={message.partition}, O={message.offset}, V={record}") # Log này có thể quá nhiều
                json_string = json.dumps(record, ensure_ascii=False)
                message_batch.append(json_string)
            except json.JSONDecodeError as json_err:
                logging.error(f"Lỗi decode JSON: {json_err}. Message P={message.partition}, O={message.offset}, Value(bytes)='{message.value}'")
            except Exception as e:
                logging.error(f"Lỗi không xác định khi xử lý message P={message.partition}, O={message.offset}: {e}")

        # Kiểm tra điều kiện ghi batch sau khi vòng lặp `for message in consumer` kết thúc (do timeout hoặc không có message)
        current_time = time.time()
        if len(message_batch) >= MAX_BATCH_SIZE or \
           (len(message_batch) > 0 and current_time - last_write_time >= MAX_TIME_INTERVAL):
            try:
                if hdfs_client is None: # Nếu client bị reset do lỗi trước đó
                    logging.warning("HDFS client is None before batch write. Re-initializing...")
                    hdfs_ready = False # Trigger re-initialization logic
                    # Phá vòng lặp chính để quay lại vòng lặp kiểm tra HDFS
                    # Hoặc bạn có thể thử khởi tạo lại client ngay tại đây.
                    # Tuy nhiên, để đơn giản, tạm thời chỉ log và có thể mất batch này nếu không được xử lý lại.
                    # Nếu bạn muốn an toàn hơn, hãy đưa message_batch vào một hàng đợi tạm.
                    logging.error("HDFS client không khả dụng, batch hiện tại có thể bị mất nếu không được xử lý lại.")
                    # Để an toàn hơn, bạn có thể break hoặc raise Exception ở đây để kích hoạt lại HDFS client
                    # Hoặc implement cơ chế lưu batch tạm thời
                    message_batch = [] # Xóa batch để tránh ghi lặp nếu lỗi tiếp diễn
                    last_write_time = current_time
                    # QUAN TRỌNG: Có thể cần phải khởi tạo lại hdfs_client ở đây hoặc thoát hẳn
                    # để đảm bảo hdfs_client luôn hợp lệ.
                    # Forcing re-check:
                    logging.info("Đang thử khởi tạo lại HDFS client ngay lập tức...")
                    # Tạm thời đặt lại hdfs_ready và break để vào vòng lặp kiểm tra HDFS
                    hdfs_ready = False
                    hdfs_client = None # Đảm bảo client được tạo mới
                    break # Thoát vòng lặp while True, để vào lại vòng kiểm tra HDFS

                write_batch_to_hdfs(hdfs_client, HDFS_PATH, message_batch)
                message_batch = []  # Xóa batch sau khi ghi thành công
                last_write_time = current_time
            except Exception as hdfs_write_err:
                logging.error(f"Lỗi trong quá trình ghi batch hoặc HDFS client không sẵn sàng: {hdfs_write_err}")
                # Khi có lỗi ghi HDFS, có thể hdfs_client đã trở nên không hợp lệ.
                # Reset nó để vòng lặp kiểm tra HDFS bên ngoài có thể tạo lại.
                hdfs_client = None
                hdfs_ready = False
                # Cân nhắc: không xóa message_batch ở đây mà thử lại sau khi client được reset.
                # Hoặc ghi vào một nơi dự phòng.
                # Để đơn giản, hiện tại batch này sẽ bị mất nếu lỗi.
                logging.warning("Batch hiện tại có thể đã bị mất do lỗi ghi HDFS.")
                message_batch = [] # Xóa để tránh ghi lặp dữ liệu cũ nếu có lỗi
                last_write_time = current_time # Reset thời gian để tránh ghi liên tục nếu lỗi
                break # Thoát vòng lặp while True, để vào lại vòng kiểm tra HDFS

        # Nếu hdfs_ready bị set lại thành False (do lỗi ghi HDFS), thoát khỏi vòng lặp này để vào vòng kiểm tra HDFS
        if not hdfs_ready:
            logging.info("HDFS không sẵn sàng, tạm dừng vòng lặp consumer và thử kết nối lại HDFS...")
            # Đóng consumer hiện tại để giải phóng tài nguyên Kafka, sẽ được tạo lại sau khi HDFS sẵn sàng
            if consumer:
                consumer.close()
                consumer = None
                logging.info("Đã đóng Kafka consumer (do HDFS không sẵn sàng).")
            # Quay lại vòng lặp kiểm tra HDFS.
            # Cần đảm bảo hdfs_client và consumer được khởi tạo lại đúng cách.
            # Vòng lặp `while not hdfs_ready:` ở đầu sẽ xử lý việc này cho hdfs_client.
            # Vòng lặp `while consumer is None:` sẽ xử lý cho consumer.
            # Ta cần break khỏi `while True` để các vòng lặp đó được thực thi.
            break


except KeyboardInterrupt:
    logging.info("Nhận tín hiệu dừng (Ctrl+C). Đang xử lý...")
finally:
    logging.info("Bắt đầu quá trình dọn dẹp và đóng ứng dụng...")
    # Ghi nốt những gì còn lại trong batch trước khi thoát
    if message_batch and hdfs_client and hdfs_ready: # Chỉ ghi nếu client còn tốt và có dữ liệu
        logging.info(f"Đang ghi nốt {len(message_batch)} messages cuối cùng vào HDFS...")
        try:
            write_batch_to_hdfs(hdfs_client, HDFS_PATH, message_batch)
            logging.info(f"Đã ghi thành công batch cuối cùng.")
        except Exception as e:
            logging.error(f"Lỗi khi ghi batch cuối cùng vào HDFS: {e}. {len(message_batch)} messages này có thể bị mất.")
    elif message_batch:
        logging.warning(f"Không thể ghi {len(message_batch)} messages cuối cùng do HDFS client không sẵn sàng.")

    if consumer:
        consumer.close()
        logging.info("Đã đóng Kafka consumer.")
    logging.info("Ứng dụng đã đóng.")