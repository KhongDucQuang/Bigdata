import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# Kết nối Elasticsearch
es = Elasticsearch("http://localhost:9200")

INDEX_NAME = "hcm_median_avg_by_month"

# Tạo index nếu chưa có
if not es.indices.exists(index=INDEX_NAME):
    es.indices.create(index=INDEX_NAME)

def read_json_lines(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():  # bỏ dòng rỗng
                doc = json.loads(line)
                yield {
                    "_index": INDEX_NAME,
                    "_source": doc
                }

json_file_path = "output/median_price/HoChiMinh.json"

# Đẩy dữ liệu
try:
    print("Đang upload JSON vào Elasticsearch...")
    bulk(es, read_json_lines(json_file_path))
    print("Đẩy thành công!")
except Exception as e:
    print(f"Lỗi khi đẩy lên Elasticsearch: {e}")
