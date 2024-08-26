from main import lifespan
from fastapi import FastAPI
from pydantic import BaseModel
from tools.prompt_tool import OpenAIPrompt


class Query(BaseModel):
    text: str


api = FastAPI(lifespan=lifespan)


@api.post("/prompt_model")
async def prompt_model(query: Query) -> str:
    openai_prompt = OpenAIPrompt()
    response = openai_prompt.generate(query.text)

    return response
