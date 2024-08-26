import json
import consts
import logging
from pymilvus import (
    connections,
    utility,
    FieldSchema,
    CollectionSchema,
    DataType,
    Collection,
    MilvusException,
    SearchResult,
    SearchFuture
)
from tools.config import MilvusConfig
from transformers import AutoTokenizer, AutoModel


logger = logging.getLogger(__file__)
MILVUS_CONFIG = MilvusConfig()


def connect() -> None:
    try:
        connections.connect(MILVUS_CONFIG.database_name, host=MILVUS_CONFIG.host, port=MILVUS_CONFIG.port)
        print(f"Connected successfully to Milvus VDB at {MILVUS_CONFIG.host}:{MILVUS_CONFIG.port}.")
    except Exception as ex:
        print(f"Failed to connect to Milvus: {ex}")
        raise


def disconnect() -> None:
    try:
        connections.disconnect(MILVUS_CONFIG.database_name)
    except Exception as ex:
        print(f"Failed to disconnect from Milvus: {ex}")
        raise


def get_meetings_fields(embedding_dim: int = 1024, auto_id_pk: bool = True) -> list[FieldSchema]:
    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=auto_id_pk),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=embedding_dim)
    ]

    return fields


def get_meetings_index():
    field_name = "embedding"
    index = {
        "index_type": "IVF_FLAT",
        "metric_type": "COSINE",
        "params": {"nlist": 128}
    }

    return field_name, index


def drop_collection(name: str) -> None:
    if utility.has_collection(name):
        utility.drop_collection(name)


def create_collection(name: str, fields: list[FieldSchema], field_index: tuple[str, dict]) -> Collection:
    schema = CollectionSchema(fields=fields)
    collection = Collection(name=name, schema=schema)
    collection.create_index(field_index[0], field_index[1])

    return collection


def _get_tokenizer(tokenizer_model=consts.TOKENIZER_MODEL_NAME) -> object:
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_model)
    return tokenizer


def _get_text_embedding_model(embedding_model_name=consts.EMBEDDING_MODEL_NAME) -> object:
    model = AutoModel.from_pretrained(embedding_model_name)
    return model


def _embedding_text(text: list[str], tokenizer, embedding_model) -> list[float]:
    batch = tokenizer(text, max_length=512, padding=True, truncation=True, return_tensors="pt")
    outputs = embedding_model(**batch)
    last_hidden = outputs.last_hidden_state.masked_fill(~batch["attention_mask"][..., None].bool(), 0.0)
    torch_embeddings_list = last_hidden.sum(dim=1) / batch["attention_mask"].sum(dim=1)[..., None]

    return torch_embeddings_list[0].tolist()


def init_vectors_store(auto_id_pk: bool) -> None:
    fields = get_meetings_fields(auto_id_pk=auto_id_pk)
    index = get_meetings_index()
    drop_collection(MILVUS_CONFIG.meeting_summaries)
    collection = create_collection(MILVUS_CONFIG.meeting_summaries, fields, index)
    collection.load()


def insert_meetings(collection_alias: str, meeting_docs_per_person: dict[str, list[str]]) -> None:
    collection = Collection(collection_alias)
    embeddings = []
    tokenizer = _get_tokenizer()
    embedding_model = _get_text_embedding_model()
    for person, docs in meeting_docs_per_person.items():
        embedding = _embedding_text(docs, tokenizer, embedding_model)
        embeddings.append({ "embedding": embedding })
    result = collection.insert(embeddings)
    print(f"Inserted {result.insert_count} meetings.")


def insert_meeting_summary(summary: str | list[str]) -> int:
    collection = Collection(MILVUS_CONFIG.meeting_summaries)
    tokenizer = _get_tokenizer()
    embedding_model = _get_text_embedding_model()
    input = summary if isinstance(summary, list) else [summary]
    embedding = _embedding_text(input, tokenizer, embedding_model)
    result = collection.insert([{"embedding": embedding}])
    print(f"Inserted {result.insert_count} meetings.")

    return result.primary_keys[0]


def delete_meeting_summary(id: int) -> None:
    collection = Collection(MILVUS_CONFIG.meeting_summaries)
    try:
        collection.delete(f"id in [{id}]")
    except MilvusException as ex:
        logger.error(f"Failed to delete meeting summary vector with id {id}")


def search(query: str, limit: int=3) -> SearchResult | SearchFuture:
    collection = Collection(MILVUS_CONFIG.meeting_summaries)
    tokenizer = _get_tokenizer()
    embedding_model = _get_text_embedding_model()
    embedded_text = _embedding_text([query], tokenizer, embedding_model)
    param = {"metric_type": "COSINE"}
    result = collection.search([embedded_text], param=param, limit=limit, anns_field="embedding")
    return result[0]


def load_meeting_summaries_embeddings(embeddings_file_path: str) -> None:
    with open(embeddings_file_path, "r") as fp:
        json_data = json.load(fp)
        collection = Collection(MILVUS_CONFIG.meeting_summaries)
        collection.insert(json_data)


def save_meeting_summaries_embeddings(dest_file_path: str, collection_alias="meeting_summaries") -> None:
    collection = Collection(collection_alias)
    rows_count = collection.query("id > 0", output_fields=["count(*)"])[0]["count(*)"]
    result = collection.query("id > 0", output_fields=["embedding"], limit=rows_count)
    # Convert numpy.float32 to float
    temp_results = [{"id": item["id"], "embedding": [float(v) for v in item["embedding"]]} for item in result]
    embeddings_json = json.dumps(temp_results)
    with open(dest_file_path, "w") as fp:
        fp.write(embeddings_json)


if __name__ == "__main__":
    connect()
    load_meeting_summaries_embeddings(consts.VECTOR_DB_EMBEDDINGS_FILE_PATH)
    disconnect()


