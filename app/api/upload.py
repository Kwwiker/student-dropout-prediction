from fastapi import APIRouter, UploadFile, File
from app.schemas.upload import UploadResponse
from app.services.file_service import FileService
from app.services.pipeline_service import PipelineService


router = APIRouter(prefix="/upload", tags=["Upload"])

# @router.post("/", response_model=UploadResponse)
# async def upload_file(file: UploadFile = File(...)):
#     saved_file = FileService.save_upload_file(file)
#
#     file_info = PipelineService.analyze_uploaded_file(
#         file_path=saved_file["file_path"],
#         extension=saved_file["extension"],
#     )
#
#     working_df, found_features, missing_features = PipelineService.build_working_dataframe(
#         file_path=saved_file["file_path"],
#         extension=saved_file["extension"],
#     )
#
#     print("FOUND FEATURES:", found_features)
#     print("MISSING FEATURES:", missing_features)
#     print("WORKING DF COLUMNS:", working_df.columns.tolist())
#     print("WORKING DF SHAPE:", working_df.shape)
#     print(working_df.head())
#
#     return UploadResponse(
#         message="Файл успешно загружен и прочитан",
#         original_filename=saved_file["original_filename"],
#         stored_filename=saved_file["stored_filename"],
#         file_path=saved_file["file_path"],
#         extension=saved_file["extension"],
#         file_info=file_info,
#     )

from fastapi import APIRouter, File, UploadFile

router = APIRouter(prefix="/upload", tags=["Upload"])


@router.post("/")
async def upload_files(
        progress_week_file: UploadFile | None = File(None),
        progress_month_file: UploadFile | None = File(None),
        activity_file: UploadFile | None = File(None),
        survey_file: UploadFile | None = File(None),
        payments_file: UploadFile | None = File(None),
):
    uploaded_files = [
        progress_week_file,
        progress_month_file,
        activity_file,
        survey_file,
        payments_file,
    ]

    files = [file for file in uploaded_files if file is not None]

    if not files:
        return {
            "message": "Не загружено ни одного файла"
        }

    saved_files = []
    for file in files:
        saved_file = FileService.save_upload_file(file)
        saved_files.append(saved_file)

    merged_df, found_features, missing_features, merged_file_path = PipelineService.build_merged_working_dataframe(
        saved_files=saved_files,
    )

    print("FOUND FEATURES:", found_features)
    print("MISSING FEATURES:", missing_features)
    print("MERGED DF COLUMNS:", merged_df.columns.tolist())
    print("MERGED DF SHAPE:", merged_df.shape)
    print(merged_df.head())

    return {
        "message": "Файлы успешно загружены и объединены",
        "files_count": len(saved_files),
        "merged_columns": merged_df.columns.tolist(),
        "merged_shape": list(merged_df.shape),
        "found_features": found_features,
        "missing_features": missing_features,
        "merged_file_path": merged_file_path,
    }
