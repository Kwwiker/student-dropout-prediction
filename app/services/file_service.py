from pathlib import Path
from fastapi import UploadFile
import shutil

from app.core.settings import settings
from app.utils.files import ensure_directory, generate_unique_filename


class FileService:
    @staticmethod
    def save_upload_file(upload_file: UploadFile):
        ensure_directory(settings.upload_dir)

        unique_filename = generate_unique_filename(upload_file.filename)
        destination: Path = settings.upload_dir / unique_filename

        with destination.open("wb") as buffer:
            shutil.copyfileobj(upload_file.file, buffer)

        return {
            "original_filename": upload_file.filename,
            "stored_filename": unique_filename,
            "file_path": str(destination),
        }
