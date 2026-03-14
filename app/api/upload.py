from http.client import HTTPException

from fastapi import APIRouter, UploadFile, File
from fastapi.openapi.utils import status_code_ranges

from app.schemas.upload import UploadResponse
from app.services.file_service import FileService
from app.services.pipeline_service import PipelineService


router = APIRouter(prefix="/upload", tags=["Upload"])


@router.post("/", response_model=UploadResponse)
async def upload_file(file: UploadFile = File(...)):
    if not file.filename:
        raise HTTPException(status_code=400, detail="Файл не был передан")

    saved_file = FileService.save_upload_file(file)

    # Заглушка
    PipelineService.process_uploaded_file(saved_file["file_path"])

    return UploadResponse(
        message="Файл успешно загружен",
        original_filename=saved_file["original_filename"],
        stored_filename=saved_file["stored_filename"],
        file_path=saved_file["file_path"]
    )