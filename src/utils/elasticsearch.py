from elasticsearch import Elasticsearch
import os

ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST")
ELASTICSEARCH_PORT = os.getenv("ELASTICSEARCH_PORT")
ELASTICSEARCH_RAW_INDEX = os.getenv("ELASTICSEARCH_RAW_INDEX")
ELASTICSEARCH_RICH_INDEX = os.getenv("ELASTICSEARCH_RICH_INDEX")

client = Elasticsearch(F'{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}',
                       headers={
        "Accept": "application/json",
        "Content-Type": "application/json"
    })

def save_raw_event(event):
    client.index(index=ELASTICSEARCH_RAW_INDEX, document=event)

def save_rich_event(event):
    # TODO
    client.index(index=ELASTICSEARCH_RICH_INDEX, document=event)