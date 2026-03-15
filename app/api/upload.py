from fastapi import APIRouter, UploadFile, File

from app.schemas.upload import UploadResponse
from app.services.file_service import FileService
from app.services.pipeline_service import PipelineService


router = APIRouter(prefix="/upload", tags=["Upload"])


@router.post("/", response_model=UploadResponse)
async def upload_file(file: UploadFile = File(...)):
    saved_file = FileService.save_upload_file(file)

    file_info = PipelineService.analyze_uploaded_file(
        file_path=saved_file["file_path"],
        extension=saved_file["extension"],
    )

    return UploadResponse(
        message="Файл успешно загружен и прочитан",
        original_filename=saved_file["original_filename"],
        stored_filename=saved_file["stored_filename"],
        file_path=saved_file["file_path"],
        extension=saved_file["extension"],
        file_info=file_info,
    )