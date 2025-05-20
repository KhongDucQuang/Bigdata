import schedule
import time
import subprocess
import logging
import os

# --- Cấu hình Logging ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("scheduler.log"),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)

# --- Cấu hình Tác vụ ---
DOCKER_COMPOSE_PROJECT_DIR = r"C:\Users\quang\Documents\20242\BTL"
SPARK_BATCH_SERVICE_NAME = "spark-batch-runner"
SCHEDULE_INTERVAL_HOURS = 2

def run_spark_batch_job():
    logger.info(f"SCHEDULER: Chuẩn bị chạy Spark batch job: {SPARK_BATCH_SERVICE_NAME}")

    if not os.path.isdir(DOCKER_COMPOSE_PROJECT_DIR):
        logger.error(f"SCHEDULER_ERROR: Thư mục dự án Docker Compose không tồn tại: {DOCKER_COMPOSE_PROJECT_DIR}")
        return

    try:
        command_to_run = ["docker-compose", "run", "--rm", SPARK_BATCH_SERVICE_NAME]

        logger.info(f"SCHEDULER: Đang thực thi lệnh: {' '.join(command_to_run)} trong thư mục {DOCKER_COMPOSE_PROJECT_DIR}")

        process = subprocess.run(
            command_to_run,
            cwd=DOCKER_COMPOSE_PROJECT_DIR,
            capture_output=True,
            text=True,
            check=False
        )

        if process.returncode == 0:
            logger.info(f"SCHEDULER_SUCCESS: Spark batch job '{SPARK_BATCH_SERVICE_NAME}' đã hoàn thành thành công.")
            if process.stdout:
                logger.info(f"--- Output của Job ---\n{process.stdout}\n--- Kết thúc Output ---")
        else:
            logger.error(f"SCHEDULER_ERROR: Spark batch job '{SPARK_BATCH_SERVICE_NAME}' thất bại với mã thoát {process.returncode}.")
            if process.stdout:
                logger.info(f"--- Output của Job (khi thất bại) ---\n{process.stdout}\n--- Kết thúc Output ---")
            if process.stderr: # Lỗi thường được ghi vào stderr
                logger.error(f"--- Lỗi của Job (stderr) ---\n{process.stderr}\n--- Kết thúc Lỗi ---")

    except FileNotFoundError:
        logger.error("SCHEDULER_ERROR: Lệnh 'docker-compose' không được tìm thấy. Hãy đảm bảo Docker Compose đã được cài đặt và nằm trong PATH của hệ thống.")
    except Exception as e:
        logger.error(f"SCHEDULER_ERROR: Đã xảy ra lỗi không mong muốn khi chạy Spark batch job: {e}", exc_info=True)

if __name__ == "__main__":

    logger.info(f"Scheduler đã khởi động. Sẽ chạy job '{SPARK_BATCH_SERVICE_NAME}' mỗi 10 phút.")
    schedule.every(10).minutes.do(run_spark_batch_job) # Mỗi 10 phút

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Scheduler đã bị dừng bởi người dùng (Ctrl+C).")
    except Exception as e:
        logger.error(f"Scheduler gặp lỗi và phải dừng: {e}", exc_info=True)
    finally:
        logger.info("Scheduler đã tắt.")