import json

# Đọc file JSON
with open('./data/data1.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Lọc các bản ghi có gia_ban > 1000
filtered_data = [record for record in data if 'gia_ban' in record and isinstance(record['gia_ban'], (int, float)) and record['gia_ban'] > 1000]

# In kết quả
for item in filtered_data:
    print(item)
