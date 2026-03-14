from pydantic import BaseModel

from app.schemas.file_info import FileStructureInfo


class UploadResponse(BaseModel):
    message: str
    original_filename: str
    stored_filename: str
    file_path: str
    extension: str
    file_info: FileStructureInfo
