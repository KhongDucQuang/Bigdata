FROM python:3.9-slim

WORKDIR /app

COPY requirements_consumer.txt .
RUN pip install --no-cache-dir -r requirements_consumer.txt

COPY kafka_consumer.py .



CMD ["python", "kafka_consumer.py"]