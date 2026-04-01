FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "/app/src/processor.py"]
#comando para levantar el contenedor: docker run -d --name processor -e ELASTICSEARCH_HOST=elasticsearch -e ELASTICSEARCH_PORT=9200 -e ELASTICSEARCH_RAW_INDEX=raw_events -e ELASTICSEARCH_RICH_INDEX=rich_events -p 8000:8000 processor