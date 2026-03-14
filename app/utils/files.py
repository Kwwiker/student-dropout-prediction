from pathlib import Path
from uuid import uuid4


def ensure_directory(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def generate_unique_filename(original_filename: str):
    extension = Path(original_filename).suffix
    return f"{uuid4()}{extension}"
