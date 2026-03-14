from pathlib import Path
from uuid import uuid4

from app.core.settings import settings


def ensure_directory(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def generate_unique_filename(original_filename: str):
    extension = Path(original_filename).suffix
    return f"{uuid4()}{extension}"


def get_file_extension(filename: str):
    return Path(filename).suffix.lower()


def is_allowed_extension(filename: str):
    return get_file_extension(filename) in settings.allowed_extensions
