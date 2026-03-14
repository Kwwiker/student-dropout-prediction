from http.client import HTTPException
from pathlib import Path
from fastapi import UploadFile, HTTPException
import shutil

from app.core.settings import settings
from app.utils.files import ensure_directory, generate_unique_filename, is_allowed_extension, get_file_extension


class FileService:
    @staticmethod
    def save_upload_file(upload_file: UploadFile):
        if not upload_file.filename:
            raise HTTPException(status_code=400, detail="Имя файла отсутствует")

        if not is_allowed_extension(upload_file.filename):
            allowed = ", ".join(sorted(settings.allowed_extensions))
            raise HTTPException(status_code=400, detail=f"Недопустимый формат файла. Разрешены: {allowed}")

        ensure_directory(settings.upload_dir)

        unique_filename = generate_unique_filename(upload_file.filename)
        destination: Path = settings.upload_dir / unique_filename

        with destination.open("wb") as buffer:
            shutil.copyfileobj(upload_file.file, buffer)

        return {
            "original_filename": upload_file.filename,
            "stored_filename": unique_filename,
            "file_path": str(destination),
            "extension": get_file_extension(upload_file.filename),
        }
