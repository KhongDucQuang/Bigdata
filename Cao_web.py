import json
import time
from datetime import datetime, timedelta
import re
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import os # Đảm bảo import os

# --- Danh sách User-Agent ---
user_agent_list = [
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.5615.49 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.5563.111 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/112.0', # Firefox on Ubuntu
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/111.0', # Firefox on Windows
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/111.0', # Firefox on Mac
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'
]
pages_per_ua = 20

# --- Cấu hình cơ bản ---
max_total_pages = 6000 # Tổng số trang tối đa muốn cào
base_url = "https://alonhadat.com.vn/can-ban-nha"
records_per_save = 50
output_filename = "Output_Alonhadat.json"
state_filename = "scraping_state.json" # File lưu trạng thái

data_list = [] # Khởi tạo danh sách dữ liệu
driver = None
current_ua_index = -1
wait = None
start_page = 1 # Trang bắt đầu mặc định

# --- Hàm đọc trạng thái (trang cuối cùng từ lần cào trước)---
def load_state(filename):
    try:
        if os.path.exists(filename):
            with open(filename, "r", encoding="utf-8") as f:
                state_data = json.load(f)
                last_page = state_data.get("last_completed_page", 0)
                print(f"Tìm thấy file trạng thái. Trang cuối cùng hoàn thành: {last_page}")
                return last_page + 1 # Trả về trang tiếp theo cần cào
        else:
            print("Không tìm thấy file trạng thái. Bắt đầu từ trang 1.")
            return 1
    except (json.JSONDecodeError, IOError, KeyError) as e:
        print(f"Lỗi khi đọc file trạng thái ({filename}): {e}. Bắt đầu lại từ trang 1.")
        return 1

# --- Hàm lưu trạng thái ---
def save_state(filename, page_num):
    try:
        state_data = {"last_completed_page": page_num}
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(state_data, f, indent=4)
    except IOError as e:
        print(f"Lỗi khi lưu file trạng thái ({filename}): {e}")

# --- Hàm đọc dữ liệu đã có ---
def load_existing_data(filename):
    try:
        if os.path.exists(filename):
            with open(filename, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
                if isinstance(existing_data, list):
                    print(f"Đã đọc {len(existing_data)} bản ghi từ file {filename}.")
                    return existing_data
                else:
                    print(f"Dữ liệu trong {filename} không phải là một danh sách JSON hợp lệ. Bỏ qua dữ liệu cũ.")
                    return []
        else:
            print(f"File dữ liệu {filename} chưa tồn tại.")
            return []
    except (json.JSONDecodeError, IOError) as e:
        print(f"Lỗi khi đọc file dữ liệu cũ ({filename}): {e}. Bắt đầu với danh sách rỗng.")
        return []

# --- Hàm xử lý ngày đăng ---
def parse_post_date(raw_date: str) -> str:
    raw_date = raw_date.lower().strip()
    try:
        if "hôm nay" in raw_date:
            date_obj = datetime.today()
        elif "hôm qua" in raw_date:
            date_obj = datetime.today() - timedelta(days=1)
        else:
            try:
                date_obj = datetime.strptime(raw_date, "%d/%m/%Y")
            except ValueError:
                 date_obj = datetime.strptime(raw_date, "%d/%m")
                 date_obj = date_obj.replace(year=datetime.today().year)
        return date_obj.strftime("%Y-%m-%d")
    except Exception:
        return datetime.today().strftime("%Y-%m-%d")

# --- Hàm lưu dữ liệu vào file JSON ---
def save_data_to_json(data, filename):
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Đã lưu tổng cộng {len(data)} bản ghi vào file: {filename}")
    except Exception as e:
        print(f"Lỗi khi ghi file JSON ({filename}): {e}")

# === BẮT ĐẦU CHƯƠNG TRÌNH ===

# 1. Đọc trạng thái để xác định trang bắt đầu
start_page = load_state(state_filename)

# 2. Đọc dữ liệu đã có từ lần chạy trước
data_list = load_existing_data(output_filename)
total_records_collected = len(data_list) # Cập nhật bộ đếm dựa trên dữ liệu đã đọc
print(f"Bắt đầu cào từ trang {start_page}. Hiện có {total_records_collected} bản ghi.")


# --- Vòng lặp chính ---
# Chạy từ trang `start_page` đã xác định đến `max_total_pages`
for page in range(start_page, max_total_pages + 1):

    target_ua_index = ((page - 1) // pages_per_ua) % len(user_agent_list)

    if target_ua_index != current_ua_index or driver is None:
        if driver is not None:
            print("Đang đóng trình duyệt cũ...")
            driver.quit()
            driver = None

        current_ua_index = target_ua_index
        current_user_agent = user_agent_list[current_ua_index]
        print(f"\n--- Trang {page}: Kích hoạt User-Agent {current_ua_index + 1}/{len(user_agent_list)} ---")
        print(f"User-Agent: {current_user_agent}")

        # Thiết lập trình duyệt headless
        chrome_options = Options()
        chrome_options.add_argument(f"user-agent={current_user_agent}")
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--proxy-server='direct://'")
        chrome_options.add_argument("--proxy-bypass-list=*")
        chrome_options.add_argument("--blink-settings=imagesEnabled=false")

        try:
             driver = webdriver.Chrome(options=chrome_options)
             wait = WebDriverWait(driver, 20)
             print("WebDriver khởi tạo thành công.")
        except Exception as e:
             print(f"Lỗi nghiêm trọng khi khởi tạo WebDriver với UA mới: {e}")
             break # Dừng hẳn nếu không khởi tạo được driver

    # --- Xây dựng URL ---
    if page == 1:
        url = base_url + ".htm"
    else:
        url = base_url + f"/trang-{page}.htm"

    print(f"Đang thu thập dữ liệu từ trang {page}: {url} (sử dụng UA {current_ua_index + 1})")

    # --- Truy cập và chờ tải trang ---
    page_successfully_processed = False # Cờ để kiểm tra trang có xử lý xong không
    try:
        driver.get(url)
        posts_container = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".content-items")))
        posts = posts_container.find_elements(By.CLASS_NAME, "content-item")

        if not posts:
             print(f"Không tìm thấy bài đăng nào trên trang {page}. Có thể đã hết trang hoặc trang trống.")
             # Nếu không có bài đăng, vẫn coi như trang này đã xử lý xong để chuyển sang trang kế
             page_successfully_processed = True
             # Cập nhật trạng thái ngay cả khi trang trống để không bị kẹt lại
             save_state(state_filename, page)
             continue

    except TimeoutException:
        print(f"Hết thời gian chờ tải bài đăng trên trang {page}. Thử tải lại...")
        # Thử tải lại trang một lần nữa
        try:
            # Có thể cần đợi một chút trước khi refresh
            time.sleep(random.uniform(1, 3))
            driver.refresh()
            posts_container = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".content-items")))
            posts = posts_container.find_elements(By.CLASS_NAME, "content-item")
            if not posts:
                print(f"Vẫn không tìm thấy bài đăng sau khi tải lại trang {page}. Bỏ qua.")
                page_successfully_processed = True # Vẫn coi là xong để đi tiếp
                save_state(state_filename, page)
                continue
        except TimeoutException:
            print(f"Hết thời gian chờ tải bài đăng trên trang {page} (lần 2). Bỏ qua trang này.")
            # KHÔNG cập nhật trạng thái vì trang này chưa xử lý xong
            continue # Sang vòng lặp tiếp theo (sẽ thử lại trang này nếu script dừng và chạy lại)
        except Exception as e_retry:
            print(f"Lỗi khi thử tải lại trang {page}: {e_retry}. Bỏ qua trang này.")
            # KHÔNG cập nhật trạng thái
            continue
    except Exception as e:
        print(f"Lỗi không xác định khi truy cập hoặc chờ tải trang {page}: {e}")
        # KHÔNG cập nhật trạng thái
        continue

    # --- Xử lý từng bài đăng ---
    posts_on_page = 0
    records_before_page = total_records_collected
    for post_index, post in enumerate(posts):
        try:

            # Ngày đăng
            raw_date = post.find_element(By.CLASS_NAME, "ct_date").text.strip()
            post_date = parse_post_date(raw_date)

            # Địa chỉ
            diachi_element = post.find_element(By.CLASS_NAME, "ct_dis")
            diachi = diachi_element.text.strip()
            lists = [item.strip() for item in diachi.split(",")]
            duong_pho, phuong_xa, quan_huyen, thanh_pho = None, None, None, None
            if len(lists) >= 1: thanh_pho = lists[-1]
            if len(lists) >= 2: quan_huyen = lists[-2].replace("Quận ", "").replace("Huyện ","").replace("Thị xã ", "").replace("Thành phố ","")
            if len(lists) >= 3: phuong_xa = lists[-3].replace("Phường ", "").replace("Xã ","").replace("Thị trấn ","")
            if len(lists) >= 4: duong_pho = ", ".join(lists[:-3]).replace("Đường ", "").replace("Phố ","")

            area, cngang, cdai, dorongduong, sotang, sophongngu, chodexe, price = None, None, None, None, None, None, None, None

            # Diện tích
            try:
                area_text = post.find_element(By.CLASS_NAME, "ct_dt").text.strip()
                match = re.search(r"([\d.,]+)\s*m", area_text)
                if match: area = float(match.group(1).replace(',', '.'))
            except (NoSuchElementException, ValueError): pass

            # Kích thước
            try:
                kthuoc = (post.find_element(By.CLASS_NAME, "ct_kt").text.strip()
                          .replace("KT:", "").replace("m", "").strip())
                if kthuoc and kthuoc != "..." and kthuoc != "---":
                     kthuoc_parts = [p.strip() for p in kthuoc.split("x") if p.strip()]
                     if len(kthuoc_parts) >= 1:
                         try: cngang = float(kthuoc_parts[0].replace(',','.'))
                         except ValueError: cngang = kthuoc_parts[0]
                     if len(kthuoc_parts) >= 2:
                         try: cdai = float(kthuoc_parts[1].replace(',','.'))
                         except ValueError: cdai = kthuoc_parts[1]
            except NoSuchElementException: pass

            # Đường trước nhà
            try:
                dorongduong_text = post.find_element(By.CLASS_NAME, "road-width").text.strip().replace("m", "")
                if dorongduong_text and dorongduong_text != '---':
                    dorongduong = float(dorongduong_text.replace(',','.'))
            except (NoSuchElementException, ValueError): pass

            # Số tầng
            try:
                sotang_text = post.find_element(By.CLASS_NAME, "floors").text.strip()
                if sotang_text and sotang_text != '---':
                    match = re.search(r"(\d+)", sotang_text)
                    if match: sotang = int(match.group(1))
            except (NoSuchElementException, ValueError): pass

            # Số phòng ngủ
            try:
                sophongngu_text = post.find_element(By.CLASS_NAME, "bedroom").text.strip()
                if sophongngu_text and sophongngu_text != '---':
                    match = re.search(r"(\d+)", sophongngu_text)
                    if match: sophongngu = int(match.group(1))
            except (NoSuchElementException, ValueError): pass

            # Chỗ để xe
            try:
                post.find_element(By.CLASS_NAME, "parking")
                chodexe = "Có"
            except NoSuchElementException:
                chodexe = "Không"

            # Giá bán
            try:
                price_text = post.find_element(By.CLASS_NAME, "ct_price").text.strip()
                if "thỏa thuận" in price_text.lower():
                    price = "Thỏa thuận"
                else:
                    price_num_str = price_text.replace('.', '')
                    match_ty = re.search(r"([\d,]+)\s*tỷ", price_num_str)
                    match_trieu = re.search(r"([\d,]+)\s*triệu", price_num_str)
                    if match_ty:
                        price = float(match_ty.group(1).replace(',', '.'))
                    elif match_trieu:
                        price = float(match_trieu.group(1).replace(',', '.')) / 1000
            except (NoSuchElementException, ValueError): pass

            post_data = {
                "ngay_dang": post_date, "duong_pho": duong_pho, "phuong_xa": phuong_xa,
                "quan_huyen": quan_huyen, "thanh_pho": thanh_pho, "dien_tich": area,
                "chieu_ngang": cngang, "chieu_dai": cdai, "duong_truoc_nha": dorongduong,
                "so_tang": sotang, "so_phong_ngu": sophongngu, "cho_de_xe": chodexe,
                "gia_ban": price
            }

            data_list.append(post_data)
            total_records_collected += 1
            posts_on_page += 1

            # --- Lưu dữ liệu định kỳ ---
            # Kiểm tra xem số lượng bản ghi *mới* có làm vượt qua mốc lưu tiếp theo không
            if total_records_collected % records_per_save == 0:
                 print(f"--- Đạt mốc {records_per_save} bản ghi (Tổng: {total_records_collected}). Lưu vào file... ---")
                 save_data_to_json(data_list, output_filename)

        except Exception as e:
            print(f"Lỗi khi xử lý chi tiết bài đăng thứ {post_index + 1} trên trang {page}: {e}")
            # Nếu có lỗi xử lý 1 bài đăng, vẫn tiếp tục xử lý các bài khác trên trang
            continue # Bỏ qua bài đăng lỗi

    # --- Kết thúc xử lý các bài đăng trên trang ---
    print(f"Hoàn thành xử lý trang {page}. Thu thập được {posts_on_page} bản ghi mới.")
    page_successfully_processed = True # Đánh dấu đã xử lý xong trang này

    # --- Cập nhật trạng thái và Lưu file lần cuối của trang (nếu có thêm dữ liệu) ---
    if page_successfully_processed:
        save_state(state_filename, page)
        if total_records_collected > records_before_page and total_records_collected % records_per_save != 0:
             save_data_to_json(data_list, output_filename)

    # --- Dừng ngắn giữa các trang ---
    sleep_time = random.uniform(2.0, 4.5)
    time.sleep(sleep_time)


# --- Kết thúc vòng lặp chính ---
print(f"\nHoàn tất quá trình cào dữ liệu đến trang {max_total_pages} (hoặc trang cuối cùng xử lý được).")
# Lưu lần cuối cùng để đảm bảo mọi thứ đều được lưu (dù có thể trùng với lần lưu cuối trang)
if data_list:
    print(f"Tổng cộng {total_records_collected} bản ghi đã được thu thập.")
    print(f"Lưu dữ liệu lần cuối vào file: {output_filename}")
    save_data_to_json(data_list, output_filename)
else:
    print("Không thu thập được bản ghi nào trong lần chạy này.")

# --- Đóng trình duyệt cuối cùng ---
if driver is not None:
    print("Đóng trình duyệt cuối cùng...")
    try:
        driver.quit()
    except Exception as e:
        print(f"Lỗi khi đóng trình duyệt: {e}")

print("Chương trình kết thúc.")