FROM python:3.9-slim

WORKDIR /app

COPY requirements_producer.txt .
RUN pip install --no-cache-dir -r requirements_producer.txt

COPY kafka_producer.py .
COPY data.json .

CMD ["python", "kafka_producer.py"]