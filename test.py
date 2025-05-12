import json
from datetime import datetime

# Đọc file JSON
with open('./data/datadoc.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Tập hợp các tháng khác nhau
cac_thang = set()

for item in data:
    ngay_str = item.get('ngay_dang')
    if ngay_str:
        try:
            ngay = datetime.strptime(ngay_str, '%Y-%m-%d')  # điều chỉnh định dạng nếu cần
            thang_nam = (ngay.year, ngay.month)
            cac_thang.add(thang_nam)
        except ValueError:
            print(f"Ngày không hợp lệ: {ngay_str}")

# Kết quả
print(f"Số lượng tháng khác nhau: {len(cac_thang)}")
print("Các tháng gồm:", cac_thang)
