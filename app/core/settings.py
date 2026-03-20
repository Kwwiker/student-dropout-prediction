from pathlib import Path
from pydantic_settings import BaseSettings

BASE_DIR = Path(__file__).resolve().parent.parent.parent


class Settings(BaseSettings):
    app_name: str = "Student Dropout Prediction API"
    upload_dir: Path = BASE_DIR / "uploads"
    export_dir: Path = BASE_DIR / "exports"
    allowed_extensions: set[str] = {".xlsx"}
    key_column_name: str = "user_id"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
