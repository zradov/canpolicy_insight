import os
from pathlib import Path

ENV_FILE_PATH = os.path.join(os.path.dirname(__file__), f".env.{os.getenv('ENV', 'dev')}")
ML_MODELS_DOWNLOAD_DIR = os.path.join(os.path.dirname(__file__), "..", "ml_models")
MEETINGS_URL = "https://www.ourcommons.ca/Committees/en/PACP/Meetings"
MEETING_EVIDENCE_URL_FORMAT = "https://www.ourcommons.ca/DocumentViewer/en/44-1/PACP/meeting-{0}/evidence"
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "output")
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
MEETINGS_DATA_FILE_PATH = os.path.join(DATA_DIR, "meetings.json")
DB_LOG_FILE_PATH = os.path.join(OUTPUT_DIR, "db.log")
MILVUS_LOG_FILE_PATH = os.path.join(OUTPUT_DIR, "milvus.log")
MAIN_LOG_FILE_PATH = os.path.join(OUTPUT_DIR, "main.log")
SUMMARIZATION_LOG_FILE_PATH = os.path.join(OUTPUT_DIR, "summarization.log")
TOKENIZER_MODEL_NAME = "facebook/bart-large-cnn"
SUMMARIZER_MODEL_NAME = "facebook/bart-large-cnn"
EMBEDDING_MODEL_NAME = "facebook/bart-large-cnn"
VECTOR_DB_EMBEDDINGS_FILE_PATH = os.path.join(DATA_DIR, "vector_embeddings.json")
SQL_DATA_FILE_PATH = os.path.join(DATA_DIR, "data.sql")

Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)