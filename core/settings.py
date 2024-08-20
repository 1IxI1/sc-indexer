import os

from pydantic_settings import BaseSettings, SettingsConfigDict

DOTENV = os.path.join(os.getcwd(), ".env")


class Settings(BaseSettings):
    config_path: str = "mainnet.json"
    db_user: str = ""
    db_password: str = ""
    db_origin_name: str = ""
    origin_cluster_addr: str = "localhost:5432"
    db_result_name: str = ""
    result_cluster_addr: str = "localhost:5432"
    localdb_file: str = "index-data.db"

    api_root_path: str = ""
    api_title: str = ""

    @property
    def origin_dsn(self) -> str:
        # postgresql+asyncpg://localhost:5432/ton_index_a
        return f"postgresql+asyncpg://{self.db_user}:{self.db_password}@{self.origin_cluster_addr}/{self.db_origin_name}"

    @property
    def result_dsn(self) -> str:
        return f"postgresql+asyncpg://{self.db_user}:{self.db_password}@{self.result_cluster_addr}/{self.db_result_name}"

    model_config = SettingsConfigDict(env_file=DOTENV, env_prefix="SCI_")


settings = Settings()
