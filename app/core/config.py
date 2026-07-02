from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    DATABASE_URL: str
    API_HOST: str = "127.0.0.1"
    API_PORT: int = 8000
    API_DEBUG: bool = True
    AZURE_CLIENT_ID: str = ""
    AZURE_TENANT_ID: str = ""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
    )


settings = Settings()