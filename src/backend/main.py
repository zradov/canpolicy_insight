from fastapi import FastAPI
from contextlib import asynccontextmanager

ml_models = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    ml_models["summarizer"] = None
    #ModelCatalog().get_llm_toolkit(tool_list=["sql"])
    yield
    ml_models.clear()
