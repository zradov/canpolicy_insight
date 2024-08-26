from tools import db_tools
from tools.config import OpenAIConfig
from openai import AsyncOpenAI, OpenAI


def get_open_ai_client(is_async: bool = False)  -> AsyncOpenAI | OpenAI:
    config = OpenAIConfig()
    client = AsyncOpenAI(api_key=config.api_key) if is_async else OpenAI(api_key=config.api_key)

    return client


async def async_text_to_sql(text: str) -> list[str]:
    client = get_open_ai_client()
    tables_schema = db_tools.get_tables_schema()
    tables_schema_sql = ";".join([v for v in tables_schema.values()])
    stream = await client.chat.completions.create(
        messages=[
            {
                "role": "user",
                "content": f"""Given the following database tables schemas: {tables_schema_sql}
Generate Mysql query for the following question and display the query only without any explanations.

Instead of using the "IN" clause and subquery use "JOIN" statement:
{text}"""
            }
        ],
        model = "gpt-4",
        stream=True
    )
    data = []
    async for chunk in stream:
        content = chunk.choices[0].delta.content
        if content:
            data.append(content)

    return data


if __name__ == "__main__":
    client = get_open_ai_client()
    tables_schema = db_tools.get_tables_schema()
    tables_schema_sql = ";".join([v for v in tables_schema.values()])
    stream = client.chat.completions.create(
        messages=[
            {
                "role": "user",
                "content": f"""Given the following database tables schemas: {tables_schema_sql}
Generate Mysql query for the following question and display the query only without any explanations.
Instead of using the "IN" clause and subquery use "JOIN" statement:
Summarize what the chair said in the last meeting?"""
            }
        ],
        model = "gpt-4",
        stream=True
    )
    for chunk in stream:
        print(chunk.choices[0].delta.content or "", end="", flush=True)
