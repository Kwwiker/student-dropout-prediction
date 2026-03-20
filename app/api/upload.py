from fastapi import APIRouter, UploadFile, File
from app.services.pipeline_service import PipelineService

router = APIRouter(prefix="/upload", tags=["Upload"])


@router.post("/")
async def upload_files(
        payments_file: UploadFile | None = File(None),
        progress_week_file: UploadFile | None = File(None),
        progress_month_file: UploadFile | None = File(None),
        activity_file: UploadFile | None = File(None),
        survey_file: UploadFile | None = File(None),
):
    uploaded_files = [
        payments_file,
        progress_week_file,
        progress_month_file,
        activity_file,
        survey_file,
    ]

    files = [file for file in uploaded_files if file is not None]

    if not files:
        return {
            "message": "Не загружено ни одного файла"
        }

    PipelineService.start_pipeline(files)

    return {
        "message": "Файлы успешно загружены и объединены",
    }
