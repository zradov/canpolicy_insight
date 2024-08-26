import os
from dotenv import dotenv_values


class Config:

    def __init__(self, key_prefix: str):
        self.config = {}
        env = dotenv_values(os.path.join(os.getcwd(), "..", "..", f".env.{os.getenv("ENV", "dev")}"))
        for key, value in env.items():
            if key.startswith(key_prefix):
                new_key_name = "_".join(key.split("_")[1:]).lower()
                self.config[new_key_name] = value

    def __getattr__(self, item):
        if item in self.config:
            return self.config[item]
        raise AttributeError


class DbConfig(Config):

    def __init__(self):
        super().__init__(key_prefix="DB_")


class MilvusConfig(Config):

    def __init__(self):
        super().__init__(key_prefix="MILVUS_")


class OpenAIConfig(Config):

    def __init__(self):
        super().__init__(key_prefix="OPENAI_")